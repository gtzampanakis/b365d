# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from b365dapp.models import EventState

class EventStateAdmin(admin.ModelAdmin):
    fields = [
        'id',
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

        'attacks_home',
        'attacks_away',
        'dangerous_attacks_home',
        'dangerous_attacks_away',
        'possession_home',
        'possession_away',
        'shots_on_target_home',
        'shots_on_target_away',
        'shots_off_target_home',
        'shots_off_target_away',

        'corners_home',
        'corners_away',
        'yellow_cards_home',
        'yellow_cards_away',
        'red_cards_home',
        'red_cards_away',

        'created_at',
    ]
    list_display = fields

admin.site.register(EventState, EventStateAdmin)
