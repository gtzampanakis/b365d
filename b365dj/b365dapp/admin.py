# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from b365dapp.models import EventState

class EventStateAdmin(admin.ModelAdmin):
    fields = [
        'event_start',

        'home_team',
        'away_team',

        'league',

        'current_home_goals',
        'current_away_goals',

        'asian_handicap',
        'asian_handicap_home_odds',
        'asian_handicap_away_odds',

        'total_line',
        'total_line_over',
        'total_line_under',
    ]
    list_display = fields

admin.site.register(EventState, EventStateAdmin)
