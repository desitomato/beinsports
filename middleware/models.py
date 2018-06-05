# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from django.db import models
import logging
from enum import Enum
from emam_gateway import EMAMGateway
from django.utils import timezone
from bein_sports import settings
from django.core.exceptions import ValidationError
from datetime import datetime

logger = logging.getLogger(__name__)
def get_current_formatted_datetime():
    return datetime.now().strftime(settings.DATETIME_DISPLAY_FORMAT)

class BasicXml(models.Model):
    house_id = models.CharField(max_length=50, blank=True, default='')
    som = models.CharField(max_length=50, blank=True, default='')
    opta_id = models.CharField(max_length=50, blank=True, default='')
    asset_id = models.CharField(max_length=50, blank=True, default='')
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(null=True, blank=True, editable=False)

    def save(self, *args, **kwargs):
        # On save, add current date on created_at

        if not self.id:
            self.created_at = timezone.now()
        else:
            self.updated_at = timezone.now()

        return super(BasicXml, self).save(*args, **kwargs)

    class Meta(object):
        verbose_name = 'Basic Xml'
        verbose_name_plural = 'Basic Xmls'

class BasicXmlLogs(models.Model):
    class Status(Enum):
        PENDING = 'pending'
        PROCESSING = 'processing'
        CONVERTED = 'converted'
        ERROR = 'error'
        POSTED = 'posted'

    status = models.CharField(
        max_length=20,
        choices=((x.value, x.name.title()) for x in Status),
        blank=True,
        default=Status.PENDING.value,
    )
    converted_xml = models.TextField(default='')
    basic_xml = models.ForeignKey(BasicXml, on_delete=models.CASCADE)
    log = models.TextField(default='')
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(null=True, blank=True, editable=False)
    class Meta(object):
        verbose_name = 'Basic Xml Logs'
        verbose_name_plural = 'Basic Xml Logs'


class OptaXml(models.Model):
    observation = models.PositiveSmallIntegerField(default=0)
    opta_id = models.CharField(max_length=50, blank=True, default='')
    home_away_score = models.CharField(max_length=50, blank=True, default='')
    is_completed = models.BooleanField(default=0)
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(null=True, blank=True, editable=False)
    last_update_timestamp = models.DateTimeField(null=True)

    class Meta(object):
        verbose_name = 'Opta Xml'
        verbose_name_plural = 'Opta Xmls'

class OptaXmlUpdates(models.Model):
    converted_xml = models.TextField(default='')
    opta_xml = models.ForeignKey(OptaXml, on_delete=models.CASCADE)
    log = models.TextField(default='')
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(null=True, blank=True, editable=False)
    class Meta(object):
        verbose_name = 'Opta Xml Updates'
        verbose_name_plural = 'Opta Xml Update'

class MetadataAttributes(models.Model):
    class FieldType(Enum):
        Time_code = 'TimeCode'
        Text = 'Text'

    custom_field_name = models.CharField(max_length=50)
    original_field_name = models.CharField(max_length=50)
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(null=True, blank=True, editable=False)
    unit_id = models.PositiveSmallIntegerField(blank=True, default=None)
    is_active = models.BooleanField(default=1)
    is_required = models.BooleanField(default=1)
    visibility = models.BooleanField(default=1)
    field_description = models.CharField(max_length=254)
    field_type = models.CharField(
        max_length=20,
        choices=((x.value, x.name.title()) for x in FieldType),
        blank=True,
        default=FieldType.Text.value,
    )
    def clean(self):
        error_message = ''
        if not self.id:
            self.unit_id = settings.UNIT_ID
            # Create metadata field
            self.custom_field_name = self.custom_field_name.lower()
            try:
                if int(settings.METADATA_GROUP_ID):
                    emam_gateway = EMAMGateway()
                    #Authenticating eMAM user
                    is_authenticated = emam_gateway.authenticate_user(settings.EMAM_USERNAME,
                                                                      settings.EMAM_PASSWORD, int(settings.UNIT_ID),
                                                                      settings.LICENSE_KEY)

                    if is_authenticated:
                        #Retreiving metadata group to get all metadata fields in it
                        res = emam_gateway.get_metadata_groups(int(settings.METADATA_GROUP_ID), True)
                        emam_gateway.recursive_dict(res)

                        # metadata ids included in given group
                        id_list = emam_gateway.meta_ids

                        if id_list:
                            #Create metadata field in eMAM director app
                            create_metadata_field = emam_gateway.update_metadata_field(
                                0,
                                self.field_description,
                                self.field_type,
                                self.custom_field_name,
                                '', self.is_required,
                                self.visibility, '1', '',
                                'Insert'
                            )

                            if create_metadata_field.ResponseId > 0:
                                try:
                                    #Removing redundancy by deleting records with same name
                                    instance = MetadataAttributes.objects.get(custom_field_name=self.custom_field_name)
                                    instance.delete()
                                except MetadataAttributes.DoesNotExist:
                                    # Not exists in DB, normal case
                                    pass
                                # logger.info(create_metadata_field.ResponseMessage)
                                #Getting standard id
                                try:
                                    get_custom_name = emam_gateway.get_metadata_fields(create_metadata_field.ResponseId)
                                    custom_name = str(
                                        get_custom_name.MetadataFields.diffgram.NewDataSet.Table.STANDARD_ID
                                    )
                                except AttributeError:
                                    raise AttributeError('Error while getting standard-id from eMAM.')

                                if custom_name:
                                    self.original_field_name = custom_name
                                    metadata_field_id = str(create_metadata_field.ResponseId)
                                    # Making array of metadata fields in same metadata group
                                    id_list.append(metadata_field_id)
                                    # Updating metadata group
                                    response = emam_gateway.update_metadata_group(int(settings.METADATA_GROUP_ID),
                                                                                  'Bein_sports', '', 'Update', True,
                                                                                  True,
                                                                                  id_list)
                                    if response.ResponseId > 0:
                                        logger.info(response.ResponseMessage)
                                    else:
                                        # Deleting metadata field from eMAM director app
                                        # Currently pending issue
                                        delete_metadata_field = emam_gateway.update_metadata_field(
                                            create_metadata_field.ResponseId,
                                            self.field_description, self.field_type, self.custom_field_name,
                                            '', self.is_required, self.visibility, '1', '', 'Delete')

                                        logger.info(delete_metadata_field.ResponseMessage)
                                else:
                                    error_message = 'Error while getting standard-id from eMAM.'
                                    logger.critical(get_custom_name.ResponseMessage)
                            else:
                                error_message = str(create_metadata_field.ResponseMessage) + \
                                    '. You need to delete it manually from eMAM director app and recreate it from middleware app.'

                        else:
                            # Invalid group-id or @not-possible: no metadata field in a metadata group of eMAM
                            error_message = '"METADATA_GROUP_ID" provided is not valid. Please recheck in settings file'
                            #logger.error(' "METADATA_GROUP_ID" provided is not valid. Please recheck in settings file')

                    else:
                        error_message = 'Authenication failed. Please verify eMAM configuration setting in settings file.'
                        #logger.error('Authenication failed')
                else:
                    error_message = ' "METADATA_GROUP_ID"  is not given. Please recheck in settings file'
                    #logger.error(' "METADATA_GROUP_ID" provided is not valid. Please recheck in settings file')
            except AttributeError as ex:
                error_message = str(ex) + '. Please recheck "#eMAM Configuration Settings" section in settings file.'
            except Exception as ex:
                #if ex.__class__.__name__ == 'AttributeError':
                error_message = ex
        else:
            self.updated_at = timezone.now()
            try:

                if int(settings.METADATA_GROUP_ID):
                    emam_gateway = EMAMGateway()
                    #Authenticating eMAM user
                    is_authenticated = emam_gateway.authenticate_user(settings.EMAM_USERNAME,
                                                                      settings.EMAM_PASSWORD, int(settings.UNIT_ID),
                                                                      settings.LICENSE_KEY)

                    if is_authenticated:
                        # Splitting metadata_field_id from original_field_name
                        original_field_name = self.original_field_name
                        r = original_field_name.split('_')
                        l = len(r)
                        field_id = r[l - 1]
                        #field_id = get_metadata_field_id(self.original_field_name)

                        #Updating metadata field
                        update_metadata_field = emam_gateway.update_metadata_field(
                            field_id,
                            self.field_description,
                            self.field_type,
                            self.custom_field_name,
                            '', self.is_required,
                            self.visibility, '1', '',
                            'Update'
                        )
                        #Todo : Currently eMAM update metadata field response is always success either metadata field id exits or not.
                        if update_metadata_field.ResponseId > 0:
                            logger.info(update_metadata_field.ResponseId)
                            logger.info(update_metadata_field)
                        else:
                            logger.info(update_metadata_field)
                            error_message = 'Server Response:' + str(
                                update_metadata_field.ResponseMessage) + 'This metadata field seems '\
                            'deleted from the eMAM director panel manually. Please re-create the metadata field with same name.'
                    else:
                        error_message = 'Authenication failed. Please verify eMAM configuration setting in settings file.'
                else:
                    error_message = ' "METADATA_GROUP_ID" provided is not given. Please recheck in settings file'
                    # logger.error(' "METADATA_GROUP_ID" provided is not valid. Please recheck in settings file')
            except AttributeError as ex:
                error_message = str(ex) + '. Please recheck "#eMAM Configuration Settings" section in settings file.'
            except Exception as ex:
                error_message = ex

        if error_message:
            raise ValidationError(error_message)
    def save(self, *args, **kwargs):
        # On save, add current date on created_at


        return super(MetadataAttributes, self).save(*args, **kwargs)

    class Meta(object):
        verbose_name = 'Metadata Attributes'
        verbose_name_plural = 'Metadata Attributes'

