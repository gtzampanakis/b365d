# -*- coding: utf-8 -*-
# Generated by Django 1.11.12 on 2018-04-13 18:26
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('b365dapp', '0004_auto_20180412_2000'),
    ]

    operations = [
        migrations.AddField(
            model_name='eventstate',
            name='game_id',
            field=models.CharField(db_index=True, default='foo', max_length=1024),
            preserve_default=False,
        ),
    ]
