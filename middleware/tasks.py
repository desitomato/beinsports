import glob
import logging
import os
import sys
import datetime
from bein_sports import settings
from middleware.helper import ingest_basic_xml, full_validation_and_ingest_process, check_media_asset, move_xml_to_process, update_file_name
from models import BasicXml, BasicXmlLogs, get_current_formatted_datetime, OptaXmlUpdates, OptaXml
from celery.schedules import crontab
from celery.decorators import periodic_task
from xml.dom.minidom import parse
import xml.dom.minidom
import helper
import concurrent.futures

logger = logging.getLogger(__name__)


@periodic_task(run_every=(crontab(hour="*", minute="*", day_of_week="*")))
def validate_xml():

    try:
        response = helper.check_settings_configuration()
        if not response[0]:
            logger.error(response[1])
        #elif settings.OPTA_ID == x:
        #   logger.error('OPTA_ID not given in settings file. Please provide it first to run the app.')
        else:
            xml_files = glob.glob(settings.PATH_TO_WATCH_FOLDER_XMLS)
            total_xmls = len(xml_files)
            if total_xmls == 0:
                logger.info("No xmls detected, exiting code.")
            else:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    executor.map(process_basic_xml, xml_files)
    except Exception as ex:
        logger.exception(ex)

def process_basic_xml(xml_to_read):
            check = False
            file_xml = os.path.basename(xml_to_read)
            try:
                opta_id = ''
                som_offset = '00:00:00:00'
                house_id = None
                response = move_xml_to_process(xml_file=xml_to_read, folder=settings.PROCESSING_FOLDER_NAME)
                if response[0] == True:
                    message = "File moved to processing folder"
                    logger.info(message)
                    xml_to_read = settings.PATH_TO_PROCESSING_FOLDER + '/' + response[1]
                    xmldoc = xml.dom.minidom.parse(xml_to_read) # Open XML document using minidom parser
                    doc_root = xmldoc.documentElement
                    if doc_root.getElementsByTagName('asset').length == 0:
                        helper.send_mail(house_id=True, error_fields=False, check=False, file_name=file_xml,
                                         basic_xml_id=None)
                        ingest_basic_xml(xml_file=xml_to_read, folder=settings.ERROR_FOLDER_NAME,
                                         house_id=False, record=False)
                    else:
                        parent = doc_root.getElementsByTagName('asset')[0]
                        try:
                            parent.removeAttribute('asset-id')
                        except Exception as ex:
                            pass
                        if parent.getElementsByTagName('custom-metadata').length == 0 or parent.getElementsByTagName('basic-metadata').length == 0 :
                            helper.send_mail(house_id=True, error_fields=False, check=False, file_name=file_xml,
                                             basic_xml_id=None)
                            ingest_basic_xml(xml_file=xml_to_read, folder=settings.ERROR_FOLDER_NAME,
                                             house_id=False, record=False)
                        else:
                            child = parent.getElementsByTagName('custom-metadata')[0]
                            child.setAttribute('set-standard-id', settings.SET_STANDARD_ID)
                            count = len(child.childNodes)

                            # Parse xml to get opta_id & som_offset
                            for index in range(0, count):
                                try:
                                    field = child.getElementsByTagName('field')[index]
                                    if som_offset == '00:00:00:00':
                                        if str(field.getAttribute("standard-id")) == settings.SOM_ID:
                                            som_offset = str(field.childNodes[0].data)
                                    if not opta_id:
                                        if str(field.getAttribute("standard-id")) == settings.OPTA_ID:
                                            opta_id = str(field.childNodes[0].data)
                                    if str(field.getAttribute("standard-id")) == settings.CONTENTTYPE:
                                        if settings.CONTENTTYPE and str(field.childNodes[0].data).lower().strip() == 'program':
                                            field.childNodes[0].data = 'Magazine Shows'
                                except Exception as ex:
                                    pass
                            # Parse xml to get house_id
                            for index in range(0, count):
                                try:
                                    field = child.getElementsByTagName('field')[index]
                                    # if str(field.getAttribute("standard-id")).lower().find('house') >= 0:
                                    if str(field.getAttribute("standard-id")) == settings.HOUSE_ID:
                                        house_id = str(field.childNodes[0].data)
                                        break
                                except Exception as ex:
                                    continue
                            try:
                                # House_id doesn't exits send error email ...
                                if house_id == None:
                                    logger.error("House_id doesn't exists in XML, moving xml to error folder.")
                                    ingest_basic_xml(xml_file=xml_to_read, folder=settings.ERROR_FOLDER_NAME,
                                                     house_id=False, record=False)
                                    helper.send_mail(house_id=False, error_fields=False, check=False, file_name=file_xml,
                                                     basic_xml_id=None)
                                else:

                                    ingest_action = 'create-new-asset'
                                    file_name = parent.getAttribute('file-name')
                                    try:
                                        res = check_media_asset(file_name, house_id)
                                        xml_obj = BasicXml.objects.get(house_id=house_id)
                                        if xml_obj.asset_id == '':  # Record exits with null asset_id
                                            logger.error(
                                                "Moving file back to watch folder. Asset_id corresponding to this House_id is not received yet."
                                                "In case error is reported on eMAM dashboard then you need to delete that particular record & place"
                                                " it again in watch folder after fixing it.")
                                            move_xml_to_process(xml_file=xml_to_read, folder=settings.XML_WATCH_FOLDER_NAME)
                                        else:  # Record exits with some asset_id
                                            if opta_id:
                                                xml_obj.opta_id = opta_id
                                                xml_obj.save()
                                            ingest_action = 'associate-metadata'

                                            asset_id = xml_obj.asset_id
                                            check = True
                                    except BasicXml.DoesNotExist:  # Record doesn't exits with house_id
                                        #file_name = parent.getAttribute('file-name')

                                        if res[0] == True:
                                            xml_obj = BasicXml(
                                                house_id=house_id, opta_id=opta_id, som=som_offset)  # Creation of new record with house_id
                                            '''
                                            if not opta_id:
                                                xml_obj = BasicXml(house_id=house_id)  # Creation of new record with house_id
                                            else:
                                                xml_obj = BasicXml(house_id=house_id, opta_id=opta_id, som=som_offset)  # Creation of new record with house_id & opta_id
                                            '''
                                            xml_obj.save()
                                            check = True
                                        elif res[0] == False:
                                            logger.info("Moving file back to watch folder.")
                                            move_xml_to_process(xml_file=xml_to_read, folder=settings.XML_WATCH_FOLDER_NAME)
                                    if check == True:
                                        xml_obj = BasicXml.objects.get(house_id=house_id)
                                        xml_logs_obj = BasicXmlLogs(basic_xml=xml_obj,
                                                         status=BasicXmlLogs.Status.PROCESSING.value)  # Entry in BasicXmlLogs table with processing status
                                        xml_logs_obj.save()
                                        log_message = '[{0}] Started reading XML for house id : {1}'.format(
                                            get_current_formatted_datetime(), house_id)
                                        helper.update_logs(record=xml_logs_obj, log_message=log_message)
                                        doc_root.setAttribute('user-key', settings.USER_KEY)
                                        if ingest_action == 'create-new-asset':
                                            parent.setAttribute('file-path', res[1])
                                        parent.setAttribute('file-action', settings.FILE_ACTION)
                                        parent.setAttribute('file-name', res[2])
                                        parent.setAttribute('ingest-action', ingest_action)
                                        if ingest_action == 'associate-metadata':
                                            parent.setAttribute('asset-id', asset_id)
                                        full_validation_and_ingest_process(count=count, child=child,
                                                                           xml_file=xml_to_read,
                                                                           xmldoc=xmldoc, house_id=house_id,
                                                                           ingest_action=ingest_action, basic_xml_id=xml_logs_obj,
                                                                           file_name=file_xml, asset_tag=parent)

                            except BasicXml.DoesNotExist:
                                logger.exception(BasicXml.DoesNotExist)
                            except Exception as ex:
                                logger.exception(ex)
                else:
                    sys.exit(0)
            except Exception as ex:
                helper.send_mail(house_id=True, error_fields=ex, check=False, file_name=file_xml,
                                 basic_xml_id=None)
                ingest_basic_xml(xml_file=xml_to_read, folder=settings.ERROR_FOLDER_NAME,
                                 house_id=False, record=False)
                logger.exception(ex)

@periodic_task(run_every=(crontab(hour="*", minute="*", day_of_week="*")))
def opta_updates(opta_id=''):

    response = helper.check_settings_configuration()
    if not response[0]:
        logger.info(response[1])
    else:
        logger.info('OPTA update method called.')
        try:
            if not opta_id: # Scheduler request

                # Fetching records having opta_id with valid asset_id
                basic_xml_objs = BasicXml.objects.exclude(opta_id='').exclude(asset_id='')

                if basic_xml_objs: # Records found in BasicXml table.
                    for basic_xml_obj in basic_xml_objs:

                        # BasicXmlLogs record fetched for using coverted basic xml.
                        basic_xml_logs_obj = BasicXmlLogs.objects.filter(basic_xml=basic_xml_obj).last()
                        process_opta_xml(basic_xml_logs_obj=basic_xml_logs_obj, basic_xml_obj=basic_xml_obj, opta_id=opta_id)
                else:
                    logger.info('No records from basic_xml found with valid opta_id/asset_id.')
            else: # Manual request for OMO pull
                basic_xml_obj = BasicXml.objects.get(opta_id=opta_id)
                basic_xml_logs_obj = BasicXmlLogs.objects.filter(basic_xml=basic_xml_obj).last()
                process_opta_xml(basic_xml_logs_obj=basic_xml_logs_obj, basic_xml_obj=basic_xml_obj, opta_id=opta_id)
        except Exception as ex:
            logger.exception(ex)

# Processing feed data for OPTA updates.
def process_opta_xml(basic_xml_logs_obj, basic_xml_obj, opta_id):
    try:
        if basic_xml_logs_obj:
            try:
                opta_xml_obj = OptaXml.objects.get(opta_id=basic_xml_obj.opta_id)
                if not opta_id: # Scheduler pull
                    if opta_xml_obj.is_completed == 1:  # OMO pull max. frequency reached.
                        #logger.info('completed')
                        return False
            except OptaXml.MultipleObjectsReturned: # Multiple records in OptaXml table.
                logger.exception('Multiple records found in OptaXML table against one opta_id.')
                return False
            except Exception as ex:
                #logger.info(ex)
                pass

            # Fetching converted basic_xml for particular record.


            # Adding markers tag
            response = helper.add_markers_tag(basic_xml_obj=basic_xml_obj, opta_id=opta_id, basic_xml_logs_obj=basic_xml_logs_obj)
        else:
            logger.info("No records in basic_xml_logs.")
    except Exception as ex:
        logger.exception(ex)