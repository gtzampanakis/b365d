# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models


class EventStateBase(models.Model):
    game_id = models.CharField(max_length=1024, db_index=True)
    event_start = models.DateTimeField(null=True, db_index=True)

    home_team = models.CharField(max_length=1024, db_index=True)
    away_team = models.CharField(max_length=1024, db_index=True)

    league = models.CharField(max_length=1024, db_index=True, null=True)

    current_home_goals = models.IntegerField(null=True)
    current_away_goals = models.IntegerField(null=True)

    asian_handicap = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    asian_handicap_home_odds = models.FloatField(null=True)
    asian_handicap_away_odds = models.FloatField(null=True)

    halftime_asian_handicap = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    halftime_asian_handicap_home_odds = models.FloatField(null=True)
    halftime_asian_handicap_away_odds = models.FloatField(null=True)

    total_line = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    total_line_over = models.FloatField(null=True)
    total_line_under = models.FloatField(null=True)

    halftime_total_line = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    halftime_total_line_over = models.FloatField(null=True)
    halftime_total_line_under = models.FloatField(null=True)

    first_asian_handicap = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    first_asian_handicap_home_odds = models.FloatField(null=True)
    first_asian_handicap_away_odds = models.FloatField(null=True)

    first_total_line = models.DecimalField(max_digits=5, decimal_places=2, null=True)
    first_total_line_over = models.FloatField(null=True)
    first_total_line_under = models.FloatField(null=True)

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

    created_at = models.DateTimeField(auto_now_add = True, db_index = True)

    class Meta:
        abstract = True


class EventState(EventStateBase):
    class Meta:
        ordering = ['-created_at']
        verbose_name = 'Event State'
        verbose_name_plural = 'Event States'


class CurrentEventState(EventStateBase):
    class Meta:
        ordering = ['created_at']
        verbose_name = 'Current Event State'
        verbose_name_plural = 'Current Event States'
