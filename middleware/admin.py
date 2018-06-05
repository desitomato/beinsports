# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

# Register your models here.
from middleware.models import *

class OptaXmlUpdatesInline(admin.TabularInline):
    model = OptaXmlUpdates
    can_delete = False
    ordering = ('-created_at',)
    exclude = ['converted_xml']
    readonly_fields = ['created_at', 'log']

    def has_add_permission(self, request, obj=None):
        return False



class BasicXmlLogsInline(admin.TabularInline):
    model = BasicXmlLogs
    can_delete = False
    ordering = ('-created_at',)
    exclude = ['converted_xml']
    readonly_fields = ['status', 'log', 'created_at' ]

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(OptaXml)
class OptaXml(admin.ModelAdmin):
    list_display = ['opta_id', 'observation','last_update_timestamp', 'is_completed', 'created_at', 'updated_at', 'omo_pull_button']
    exclude = []
    readonly_fields = ('home_away_score', 'opta_id', 'observation','last_update_timestamp', 'is_completed', 'created_at', 'updated_at')
    inlines = [OptaXmlUpdatesInline]
    def omo_pull_button(self, obj):
        if obj.is_completed == 1:
            return '<a href="update-request?opta_id={0}">Pull Update</a>'.format(obj.opta_id)

    omo_pull_button.allow_tags = True
    def has_add_permission(self, request, obj=None):
        return False

@admin.register(BasicXml)
class BasicXml(admin.ModelAdmin):
    list_display = ['house_id','asset_id' ,'opta_id', 'som', 'created_at', 'updated_at']
    exclude = []
    readonly_fields = ('house_id', 'created_at', 'updated_at', 'opta_id', 'som')

    inlines = [BasicXmlLogsInline]

    def has_add_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return True

@admin.register(MetadataAttributes)
class MetadataAttributes(admin.ModelAdmin):
    list_display = ['custom_field_name','original_field_name', 'is_active', 'created_at', 'updated_at']
    exclude = ['is_active', 'unit_id']
    readonly_fields = ('created_at', 'updated_at', 'original_field_name')

    def get_readonly_fields(self, request, obj=None):
        if obj:  # editing an existing object
            return self.readonly_fields + ('custom_field_name', 'unit_id', 'field_type')
        return self.readonly_fields

    def has_add_permission(self, request, obj=None):
        return True

    def has_delete_permission(self, request, obj=None):
        return True
