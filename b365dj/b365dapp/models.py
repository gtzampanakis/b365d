# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# {'asian_handicap_home_odds': 2.0, 'league': u'
# Algeria Youth League', 'home_team': u'JSM Bejaia U21', 'away_team': u'MO
# Bejaia U21', 'current_home_goals': u'0', '
# asian_handicap_away_odds': 1.8, 'total_line_over': 2.5, 'asian_handicap':
# u'0', 'event_start': datetime.datetime(20
# 18, 4, 6, 12, 13, 43), 'total_line_under': 1.5, 'current_away_goals': u'1',
# 'total_line': u'2.5'}


class EventState(models.Model):
    event_start = models.DateTimeField(null=True, db_index=True)

    home_team = models.CharField(max_length=1024, db_index=True)
    away_team = models.CharField(max_length=1024, db_index=True)

    league = models.CharField(max_length=1024, db_index=True, null=True)

    current_home_goals = models.IntegerField(null=True)
    current_away_goals = models.IntegerField(null=True)

    asian_handicap = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    asian_handicap_home_odds = models.FloatField(null=True)
    asian_handicap_away_odds = models.FloatField(null=True)

    total_line = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    total_line_over = models.FloatField(null=True)
    total_line_under = models.FloatField(null=True)

    attacks_home = models.IntegerField(null = True)
    attacks_away = models.IntegerField(null = True)
    dangerous_attacks_home = models.IntegerField(null = True)
    dangerous_attacks_away = models.IntegerField(null = True)
    possession_home = models.FloatField(null = True)
    possession_away = models.FloatField(null = True)
    shots_on_target_home = models.IntegerField(null = True)
    shots_on_target_away = models.IntegerField(null = True)
    shots_off_target_home = models.IntegerField(null = True)
    shots_off_target_away = models.IntegerField(null = True)

    corners_home = models.IntegerField(null = True)
    corners_away = models.IntegerField(null = True)
    yellow_cards_home = models.IntegerField(null = True)
    yellow_cards_away = models.IntegerField(null = True)
    red_cards_home = models.IntegerField(null = True)
    red_cards_away = models.IntegerField(null = True)

