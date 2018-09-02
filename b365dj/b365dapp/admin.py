# -*- coding: utf-8 -*-


import datetime
import os

import xlsxwriter

from django.conf import settings
from django.contrib import admin
from django.http.response import HttpResponse
from b365dapp.models import (
    EventStateBase,
    EventState,
    CurrentEventState,
)


def export_selected(modeladmin, request, queryset):
    suffix = (
        datetime.datetime.utcnow().isoformat()
        .replace('-', '.')
        .replace(':', '.')
        .replace(' ', '.')
        .replace('T', '.')
    ) + '.xlsx'
    filename = (
        'Export_' +
        (modeladmin.model._meta.verbose_name + '_' + suffix).replace(' ', '_'))
    path = os.path.join(settings.EXPORT_DIR, filename)

    wb = xlsxwriter.Workbook(path)
    date_format = wb.add_format({'num_format': 'yyyy-mm-dd hh:mm:ss'})
    worksheet = wb.add_worksheet()

    fields = [f.name for f in modeladmin.model._meta.get_fields()]

    for field, size in {
        'period_start': 20,
        'created_at': 20,
        'home_team': 30,
        'away_team': 30,
        'league': 30,
    }.items():
        worksheet.set_column(
            fields.index(field),
            fields.index(field),
            size,
        )

    for fieldi, field in enumerate(fields):
        worksheet.write(0, fieldi, field)
    
    for recordi, record in enumerate(queryset, 1):
        for fieldi, field in enumerate(fields):
            val = getattr(record, field, '')
# Order is important here because datetime is a date but date is not a
# datetime...
            if isinstance(val, datetime.datetime):
                worksheet.write(recordi, fieldi, val.replace(tzinfo=None), date_format)
            elif isinstance(val, datetime.date):
                worksheet.write(recordi, fieldi, val, date_format)
            else:
                worksheet.write(recordi, fieldi, val)
    wb.close()

    fo = open(path, 'rb')

    response = HttpResponse(
		fo,
		content_type = 
			'application/'
			'vnd.openxmlformats-officedocument.'
			'spreadsheetml.sheet'
	)
    response['Content-Disposition'] = 'attachment; filename=%s' % filename
    return response

export_selected.short_description = "Export selected records to Excel"


class EventStateBaseAdmin(admin.ModelAdmin):
    fields = [f.name for f in EventState._meta.get_fields()]
    list_display = fields
    actions = [export_selected]


class EventStateAdmin(EventStateBaseAdmin):
    pass


class CurrentEventStateAdmin(EventStateBaseAdmin):
    list_per_page = 2000


admin.site.register(EventState, EventStateAdmin)
admin.site.register(CurrentEventState, CurrentEventStateAdmin)
