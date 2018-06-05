import os
import urllib
import win32file
import logging
import time

import xml
import datetime
import  fnmatch
from bein_sports import settings
from models import MetadataAttributes, BasicXmlLogs, get_current_formatted_datetime, OptaXmlUpdates, OptaXml
from django.core.mail import EmailMessage

logger = logging.getLogger(__name__)

# Check media asset exists in media folder
def check_media_asset(file_name,house_id):
    video_folders = settings.VIDEO_FOLDER_NAMES
    file_path = ''
    count =0
    for i in range(0, video_folders.__len__()):
        full_path = settings.ROOT_FOLDER_PATH + '/' + video_folders[i] + '/' + str(file_name)
        file_path = os.path.dirname(full_path)
        for file in os.listdir(file_path):
            if fnmatch.fnmatch(file, '*'+ house_id +'*'):
                logger.info(file)
                count += 1
                file_name = file
        if count == 1:
            logger.info('Associated Media asset exists.')
            return [True, file_path, file_name]
        elif count > 1:
            logger.error("More than one media asset found with this name: ".format(file))
        else:
            logger.error("Associated Media asset doesn't exist.")
    return [False, file_path, file_name]

# Update file name
def update_file_name(file_name):
    var_1 = file_name.split('_')
    if len(var_1) > 1:
        var_2 = var_1[1].split('.')
        if len(var_2) > 1:
            file_name = var_1[0] + '.' + var_2[1]
    return file_name

# Validate XML
def update_standard_id(count, child, xml_file, xmldoc, asset_tag):
    c = 0
    r = count - 1
    check = True
    error_fields = []
    values = get_categories(child=child, count=count)
    if type(values) == bool:
        category_response = create_category_element(no_category=True, asset_tag=asset_tag, xmldoc=xmldoc)
    else:
        if values[0] == True:
            category_response = create_category_element(counter=values[1], genre=values[2], sub_genre=values[3]
                                                        , content_type=values[4], asset_tag=asset_tag, xmldoc=xmldoc)
    '''else:
        category_response = [values[0], values[1]]'''
    for index in range(0, count):
        try:
            msg = " : Metadata Field doesn't exists in 'Middleware' or its required value is missing"
            field = child.getElementsByTagName('field')[index]

            if check == True:
                if category_response[0] == False:
                    c = c + 1
                    check = False
                    error_fields.append(category_response[1])
            '''
            custom_name = MetadataAttributes.objects.get(
                original_field_name=str(field.getAttribute("standard-id")))
            if custom_name.is_required == True:
                try:
                    if str(field.childNodes[0].data):
                        logger.info(str(field.childNodes[0].data))
                except Exception as ex:
                    c = c + 1
                    error_fields.append(str(field.getAttribute('standard-id'))+msg)'''
            #field.setAttribute("standard-id", custom_name.original_field_name)
            if index == r:
                if c==0:
                    write_to_xml(xml_file=xml_file, xmldoc=xmldoc, child=child, asset_tag=asset_tag)
                    return [c,error_fields]
                if c > 0:
                    #error_fields.append(str(field.getAttribute('standard-id'))+msg)
                    return [c,error_fields]

        except MetadataAttributes.DoesNotExist:
            c = c + 1
            #error_fields.append(str(field.getAttribute('standard-id'))+msg)
            if index == r:
                return [c,error_fields]
            continue
        except Exception as ex:
            if index == r:
                if c==0:
                    write_to_xml(xml_file=xml_file, xmldoc=xmldoc, child=child, asset_tag=asset_tag)
                    return [c,error_fields]
                if c>0:
                    return [c,error_fields]
            continue


# Update  BasicXml model with converted XML.
def update_basic_xml_table(house_id, xmldoc, record):
    try:
        obj = record
        obj.converted_xml = xmldoc.toxml()
        obj.status = BasicXmlLogs.Status.CONVERTED.value
        obj.save()
        logger.info('Basic_xml updated successfully.')
        return True
    except Exception as ex:
        logger.exception(ex)
        return False

#Ingest basic_xml to ingest/error folder and set status to posted/error.
def ingest_basic_xml(xml_file, folder, house_id, record):

    xml_obj = record
    try:
        if folder == 'ingest':
            # Rename file if other file with same name exits.
            rename = rename_file(xml_file=xml_file, path=settings.EMAM_INGEST_FOLDER_FULL_PATH)
            try:
                # Converted case: XML validated successfully.
                win32file.MoveFileWithProgress(xml_file, settings.EMAM_INGEST_FOLDER_FULL_PATH + '/' + rename,
                                               None, None,
                                               0, None)
                if xml_obj:
                    xml_obj.status = BasicXmlLogs.Status.POSTED.value
                    xml_obj.save()
                logger.info('Basic_xml moved to ingest folder.')
                return True
            except Exception as ex:
                logger.exception(ex)
                return False
        if folder == settings.ERROR_FOLDER_NAME:
            try:
                # Rename file if other file with same name exits.
                rename = rename_file(xml_file=xml_file, path=settings.ROOT_FOLDER_PATH + '/' + settings.ERROR_FOLDER_NAME)
                # Processing case: XML validation fails.
                win32file.MoveFileWithProgress(xml_file, settings.ROOT_FOLDER_PATH + '/' + folder + '/' + rename,
                                               None, None,
                                               0, None)
                if house_id != False:
                    if xml_obj:
                        xml_obj.status = BasicXmlLogs.Status.ERROR.value
                        xml_obj.save()
                        logger.info('Basic_xml moved to error folder.')
                        return True
            except Exception as ex:
                logger.exception(ex)
                return False
    except Exception as ex:
            logger.exception(ex)
            return False

# Update XML file with converted XML & remove <custom-metadata> when update came only for <basic-metadata> .
def write_to_xml(xml_file, xmldoc ,child, asset_tag):
    try:
        field = child.getElementsByTagName('field')
        if len(field) == 1:
            if asset_tag.getAttribute('ingest-action') == 'associate-metadata':
                asset_tag.removeChild(child)
        file = open(xml_file, 'w')
        file.write(xmldoc.toxml())
        file.close()
        time.sleep(2)
        logger.info('Standard_id updated successfully.')
    except Exception as ex:
        logger.exception(ex)

# Move XML file to processing folder from watch folder & vice-versa.
def move_xml_to_process(xml_file, folder):
    # Rename file if other file with same name exits.
    rename = os.path.basename(xml_file)
    if folder == settings.PATH_TO_PROCESSING_FOLDER:
        rename = rename_file(xml_file=xml_file, path=settings.PATH_TO_PROCESSING_FOLDER)
    try:
        win32file.MoveFileWithProgress(xml_file, settings.ROOT_FOLDER_PATH + '/' + folder + '/' + rename, None, None, 0, None)
        return [True, rename]
    except Exception as ex:
        return [False, rename]

# Update logs in BasicXmlLogs model.
def update_logs(record, log_message=None):
    if log_message is not None:
        record.log = record.log + log_message + "\n"
        record.save()
    return record

# Container of functions to be called while processing XML.
def full_validation_and_ingest_process(count, child, xml_file, xmldoc, house_id, ingest_action, basic_xml_id, file_name, asset_tag):
    try:
        res = update_standard_id(count=count, child=child, xml_file=xml_file,
                                 xmldoc=xmldoc, asset_tag=asset_tag)
        if res[0] > 0:  # error
            log_message = "[{0}] Errors encountered while validating XML.".format(
                get_current_formatted_datetime())
            update_logs(record=basic_xml_id, log_message=log_message)
            send_mail(house_id=house_id,error_fields=res[1], check=True, file_name=file_name,basic_xml_id=basic_xml_id)
            ingest_basic_xml(xml_file=xml_file, folder=settings.ERROR_FOLDER_NAME, house_id=house_id, record=basic_xml_id)
        elif res[0] == 0:  # validated

            log_message = "[{0}] XML validated successfully.".format(
                get_current_formatted_datetime())
            update_logs(record=basic_xml_id, log_message=log_message)
            resp = update_basic_xml_table(house_id=house_id, xmldoc=xmldoc, record=basic_xml_id)
            if resp == True:
                log_message = "[{0}] XML updated successfully.".format(
                    get_current_formatted_datetime())
                update_logs(record=basic_xml_id, log_message=log_message)
            resp = ingest_basic_xml(xml_file=xml_file, folder='ingest', house_id=house_id, record=basic_xml_id)
            if resp == True:
                log_message = "[{0}] XML posted successfully.".format(
                    get_current_formatted_datetime())
                update_logs(record=basic_xml_id, log_message=log_message)
        else:
            logger.info(res)

    except Exception as ex:
        logger.exception(ex)

# Send email when error occurs while validating XML.
# In settings.py file mention the email-id where you want to recieve errror emails
def send_mail(house_id, error_fields, check, file_name, basic_xml_id):
    title = ''
    body = ''
    errors = ''
    try:
        greeting = "Hey!{0}{0}{1} You got this email because middleware has encountered some errors while validating XML. {0}{0}" \
                   "{2}Description of XML encountered with error(s) is given below :- {0}{0}{1}".format('\n', '   ', '    ')
        if check == True:

            for x in range(0, len(error_fields)):
                errors +='   ' + str(x+1) + '. ' + error_fields[x]+ '\n'
            title ='Error encountered in XML with House id : {0}'.format(house_id)
            body = greeting + "{5}File Name : {4}{3}{3}{6}Date Time : [{0}]{3}{3}{6}House id : {1}{3}{3}{6}Errors :{3}{3}{2}".format(get_current_formatted_datetime(), house_id, errors,'\n', file_name, ' ', '    ')

        if house_id == False:
            title = "House id doesn't exists in XML"
            body = greeting + "{3}Error : There is no house id field found in the XML.{0}{0}{2} File Name : {1}".format('\n', file_name, '   ', ' ')

        if house_id == True:
            title = "Invalid XML"
            if error_fields == False:
                body = greeting + "{3}Error : Invalid XML, encountered XML is not as specified format.{0}{0}{2} File Name : {1}".format('\n', file_name, '   ', ' ')
            else:
                body =greeting + "{3}Error : Invalid XML,{4}.{0}{0}{2} File Name : {1}".format(
                    '\n', file_name, '   ', ' ', error_fields)
        note = '\nNOTE: Before placing XML back to watch folder, please take care of the following points:' \
               '\n 1) Check the reported error and correct it in XML accordingly.' \
               ' \n 2) In Middleware admin panel, check if this particular `house-id` record exists in BasicXML lists but asset-id is not present. ' \
               ' \n Direct link: "http://{middleware-ip or domain}/admin/middleware/basicxml/", ' \
               ' then you need to either update the asset-id or delete this house_id record as per the following assumption : ' \
               '\n If linked video is ingested to eMAM (need to check manually in eMAM Director App), then please update the `asset-id` ' \
               ' in Middleware admin panel against that BasicXML record. Otherwise, delete the BasicXML record.'
        body += '\n' + '\n' + '    The file has been moved here ' + settings.PATH_TO_ERROR_FOLDER\
                + '. Please fix the error & move it to "XML WATCH" folder to process it again.' + '\n' + note
        logger.error("[{0}] INFO: {1}.".format(get_current_formatted_datetime(), body))
        email = EmailMessage(title, body, to=[settings.EMAIL_RECEIVER])
        email.send()
    except Exception as ex:
        logger.exception(ex)
        log_message = "[{0}] WARNING: {1}.".format(get_current_formatted_datetime(), ex)
        if basic_xml_id is not None:
            update_logs(record=basic_xml_id, log_message=log_message)

# Rename file if file with same name already exits.
def rename_file(xml_file, path):
    rename = os.path.basename(xml_file)
    res = os.path.isfile(path + '/' + rename)
    # Rename file if other file with same name exits.
    if res == True:
        base_name = os.path.basename(xml_file)
        b = base_name.split('.')
        rename = b[0] + '_' + str(int(round(time.time()))) + '.' + b[1]
        return rename
    return rename

# Getting values of fields 'GENRE', 'SUBGENRE', 'CONTENTTYPE'
def get_categories(child, count):
    try:
        counter = 0
        genre = remove_element_from_xml(count=count, child=child, category='GENRE', counter=counter)
        sub_genre= remove_element_from_xml(count=count, child=child, category='SUBGENRE', counter=genre[1])
        content_type = remove_element_from_xml(count=count, child=child, category='CONTENTTYPE', counter=sub_genre[1])
        if genre[0] == False or sub_genre[0] == False or content_type[0] == False:
            message = 'Value of one of the following fields "GENRE, SUBGENRE, CONTENTTYPE" is missing.'
            return True
        else:
            return [True, content_type[1], genre[0], sub_genre[0], content_type[0]]
    except Exception as ex:
        logger.exception(ex)

# Removing fields GENRE, SUBGENRE, CONTENTTYPE from XML.
def remove_element_from_xml(count ,child, category, counter):
    try:
        category_value = False
        for index in range(0, count):
            try:
                field = child.getElementsByTagName('field')[index]
                if str(field.getAttribute("standard-id")) == category:
                    counter = counter + 1
                    category_value = str(field.childNodes[0].data)
                    if category == 'CONTENTTYPE' :
                        if category_value.lower().strip() == 'program':
                            category_value = 'Magazine Shows'
                    child.removeChild(field)
                    break
            except Exception as ex:
                continue
        remove_empty_value_fields(count=count, child=child)
        return [category_value, counter]
    except Exception as ex:
        logger.exception(ex)

# Add <categories> element in XML.
def create_category_element(asset_tag, xmldoc, counter=0, genre=0, sub_genre=0, content_type=0, no_category=False):
    try:
        if no_category == True:
            category = settings.EMAM_CATEGORY_ROOT_FOLDER
        else :
            # Creation of new element "categories" and its sub-element 'category' and its attribute 'name'.
            if counter == 3:
                category = settings.EMAM_CATEGORY_ROOT_FOLDER + '/' + str(genre) + '/' + str(sub_genre) + '/' + str(content_type)
            else:
                category = settings.EMAM_CATEGORY_ROOT_FOLDER

        new_element = xmldoc.createElement("categories")
        sub_element = xmldoc.createElement('category')
        sub_element.setAttribute('name', category)
        new_element.appendChild(sub_element)
        asset_tag.appendChild(new_element)
        return [True, True]
    except Exception as ex:
        logger.exception(ex)
        message = 'Value of one of the following fields "GENRE, SUBGENRE, CONTENTTYPE" is missing.'
        return [False, message]

# Parse xml to remove fields with no values
def remove_empty_value_fields(count, child):

    for index in range(0, count):
        try:
            field = child.getElementsByTagName('field')[index]
            try:
                field_value = field.childNodes[0].data
            except Exception as ex:
                child.removeChild(field)
        except Exception as ex:
            continue

# Add <markers> element in XML.
def add_markers_tag(basic_xml_obj, opta_id, basic_xml_logs_obj):
    count = 0
    custom_count = 0
    try:
        try:
            opta_feed_data = urllib.urlopen('http://omo.akamai.opta.net/?game_id={0}&feed_type=F13&user=BESbeIN&psw=rFr2cLfldA1a&language=en'.format(basic_xml_obj.opta_id))
        except Exception as ex:
            logger.error('Error while hitting OMO url for F13 feed.')
            return False
        xmldoc = xml.dom.minidom.parse(opta_feed_data)
        doc_root = xmldoc.documentElement

        if doc_root.getElementsByTagName('message'): # Feed data avaialbale for particular game_id

            home_score = str(doc_root.getAttribute('home_score')) + ''
            away_score = str(doc_root.getAttribute('away_score')) + ''

            try:
                opta_xml_obj = OptaXml.objects.get(opta_id=basic_xml_obj.opta_id)

            except OptaXml.DoesNotExist: # No record found in OptaXml table.

                # Creation of new record in OptaXml table.
                logger.info('No record from opta_xml found. Going to insert record.')
                opta_xml_obj = OptaXml(opta_id=basic_xml_obj.opta_id)
                opta_xml_obj.save()
                logger.info('opta record created.')

            except OptaXml.MultipleObjectsReturned: # Multiple records in OptaXml table.
                logger.exception('Multiple records found in OptaXML table against one opta_id.')
                return False
            if not opta_id:  # If scheduler OMO pull only then return.
                if opta_xml_obj.is_completed == 1:  # OMO pull max. frequency reached.
                        return False
                # OMO pull max. frequency not reached yet.
                if opta_xml_obj.last_update_timestamp: # This one is not the first hit.

                    # First check its time for next OMO pull or not.
                    # Calculating difference b/w current & last_time_stamp.
                    current_time = datetime.datetime.now()
                    diff_in_time = current_time - opta_xml_obj.last_update_timestamp
                    diff_in_time = diff_in_time.total_seconds()
                    diff_in_time = diff_in_time / 60

                    if not diff_in_time >= settings.GAP_BETWEEN_OMO_PULLS:
                        logger.info('Some time left for next OMO pull.')
                        return False

            # Creation of opta_xml in processing folder
            opta_xml = create_opta_xml(basic_xml_logs_obj=basic_xml_logs_obj)
            # Creation of new OptaXmlUpdates record.
            opta_xml_obj.last_update_timestamp = datetime.datetime.now()
            opta_xml_obj.save()
            opta_update_obj = create_optaxmlupdates_record(opta_xml_obj)

            # Fetching SOM offset from database
            som_offset_1 = basic_xml_obj.som

            som_offset_2 = settings.SOM_VALUES['f13_second_half']
            '''
            # Updating SOM-offset in database.
            try:
                som_offset = som_value(opta_xml[0])
                basic_xml_obj.som = som_offset
                basic_xml_obj.save()
            except Exception as ex:
                logger.exception(ex) '''
            update_logs(record=opta_update_obj, log_message=opta_xml[1])

            message_tags = doc_root.getElementsByTagName('message')
            score_field_count = 0
            season_id = str(doc_root.getAttribute('season_id'))
            competition_id = str(doc_root.getAttribute('competition_id'))

            #OMO pull for getting players names.
            try:
                players_feed_data = urllib.urlopen('http://omo.akamai.opta.net/competition.php?season_id={0}&competition={1}&feed_type=F40&user=BESbeIN&psw=rFr2cLfldA1a'.format(season_id, competition_id))
            except Exception as ex:
                error_message='Error while hitting OMO url for F40 feed.'
                logger.error(error_message)
                update_logs(record=opta_update_obj, log_message=error_message)
                return False

            player_names = get_players_name(players_feed_data)
            if player_names[0] == True:  # Players list fetched successfully
                FIRST_HALF_END_CHECK = 0

                for message_tag in message_tags:
                    try:
                        try:
                            min = int(message_tag.getAttribute('minute'))
                            sec = int(message_tag.getAttribute('second'))
                        except Exception as ex:
                            min = 0
                            sec = 0
                        message_type = str(message_tag.getAttribute('type')) + ''
                        message_id = str(message_tag.getAttribute('id')) + ''
                        player_1 = 'p' + str(message_tag.getAttribute('player_ref1'))
                        player_2 = 'p' + str(message_tag.getAttribute('player_ref2'))
                        comment_text = message_tag.getAttribute('comment')#.encode('ascii', 'ignore')

                        # Calculating SOM for first half
                        x = time.strptime(som_offset_1, '%H:%M:%S:%f')
                        som_1 = int(datetime.timedelta(hours=x.tm_hour, minutes=x.tm_min,
                                                       seconds=x.tm_sec).total_seconds())

                        # Calculating SOM for second half
                        y = time.strptime(som_offset_2, '%H:%M:%S:%f')
                        som_2 = int(datetime.timedelta(hours=y.tm_hour, minutes=y.tm_min,
                                                     seconds=y.tm_sec).total_seconds())

                        if message_type == 'end 1':
                            FIRST_HALF_END_CHECK = FIRST_HALF_END_CHECK + 1
                        if FIRST_HALF_END_CHECK == 1: # First half starts, reverse iteration
                            total_sec = min * 60 + sec + som_1
                        elif FIRST_HALF_END_CHECK == 0: # Second half
                            total_sec = min * 60 + sec + som_1 + som_2

                        time_code = str(datetime.timedelta(seconds=total_sec)) + ':00'
                        if len(time_code) == 10:
                            time_code = '0' + str(datetime.timedelta(seconds=total_sec)) + ':00'
                        xmldoc = xml.dom.minidom.parse(opta_xml[0])
                        doc_root = xmldoc.documentElement
                        asset_tag = doc_root.getElementsByTagName('asset')[0]
                        asset_tag.setAttribute('ingest-action', 'associate-metadata')
                        asset_tag.setAttribute('asset-id', str(basic_xml_obj.asset_id))
                        try:
                            asset_tag.removeAttribute('file-action')
                        except Exception as ex:
                            pass
                        try:
                            asset_tag.removeAttribute('file-path')
                        except Exception as ex:
                            pass
                        try:
                            category_tag = asset_tag.getElementsByTagName('categories')[0]
                            asset_tag.removeChild(category_tag)
                        except Exception as ex:
                            pass
                        if custom_count == 0:
                            custom_metadata = asset_tag.getElementsByTagName('custom-metadata')[0]

                        # creation of new <field> tag for score within <custom> tag
                        if score_field_count == 0:
                            score = home_score + away_score
                            score_value = xmldoc.createTextNode(score)
                            if not home_score == '' or away_score == '':
                                score = home_score + '-' + away_score
                                score_value = xmldoc.createTextNode(score)

                            # Removing <custom-metadata> if score in f13 feed is not updated.
                            if opta_xml_obj.home_away_score == score and custom_count == 0:
                                asset_tag.removeChild(custom_metadata)
                                custom_count = custom_count +1
                            else:
                                # Updating score
                                opta_xml_obj.home_away_score = score
                                opta_xml_obj.save()
                                if custom_count == 0:
                                    new_field = xmldoc.createElement('field')
                                    new_field.setAttribute('standard-id', settings.SCORE_ID)
                                    new_field.appendChild(score_value)
                                    custom_metadata.appendChild(new_field)
                                    score_field_count = + 1

                        try:
                            new_element = asset_tag.getElementsByTagName('markers')[0]
                        except Exception as ex:
                            new_element = xmldoc.createElement("markers")

                        sub_element = xmldoc.createElement('marker')
                        sub_element.setAttribute('duration', settings.MARKER_DURATION)
                        sub_element.setAttribute('name', message_id)
                        count = count + 1
                        sub_element.setAttribute('time-code', time_code)
                        try:
                            if player_1 == 'p':  # player_1 doesn't exits means player_2 also not exits.
                                comment_text = message_type.title() + '\n' + comment_text
                            else:  # player_1 exits
                                if player_2 == 'p':
                                    comment_text = message_type.title() + ': ' + player_names[1][
                                        player_1] + '\n' + comment_text
                                else:  # both exits
                                    comment_text = message_type.title() + ': ' + player_names[1][player_1] + ' / ' + \
                                                   player_names[1][player_2] + '\n' + comment_text
                        except Exception as ex:
                            logger.exception(ex)
                        comment = xmldoc.createTextNode(comment_text)
                        try:
                            sub_element.appendChild(comment)
                        except Exception as ex:
                            logger.exception(ex)
                        new_element.appendChild(sub_element)
                        asset_tag.appendChild(new_element)
                        file = open(opta_xml[0], 'w')
                        file.write(xmldoc.toxml().encode('utf-8'))
                        file.close()

                    except Exception as ex:
                        if ex.__class__.__name__ == 'ValueError':
                            logger.info(ex)
                        else:
                            logger.exception(ex)
                            continue
                log_message = "[{0}] Opta XML converted successfully.".format(
                    get_current_formatted_datetime())
                logger.info(log_message)
                update_logs(record=opta_update_obj, log_message=log_message)
                change_status_is_completed(opta_xml_obj, opta_id)
                success_check = move_opta(xml_file=opta_xml[0], folder='ingest')
                if success_check[1] == True:
                    log_message = "[{0}] Opta XML posted successfully.".format(
                        get_current_formatted_datetime())
                    logger.info(log_message)
                    update_logs(record=opta_update_obj, log_message=log_message)
                else:
                    log_message = "[{0}] Error while posting opta XML.".format(
                        get_current_formatted_datetime())
                    logger.error(log_message)
                    update_logs(record=opta_update_obj, log_message=log_message)

            else:  # Player list not fetched successfully.
                error_message = "Player list fetching fails from f40 feed."
                logger.error(error_message)
                update_logs(record=opta_update_obj, log_message=error_message)
        else:
            logger.error('F13 feed data not avaialbale for particular game_id')
    except Exception as ex:
        logger.exception(ex)

# Moving opta_xml to ingest folder.
def move_opta(xml_file, folder):
    try:
        if folder == "ingest":
            path = settings.EMAM_INGEST_FOLDER_FULL_PATH
        else:
            path = settings.PATH_TO_PROCESSING_FOLDER
        rename = rename_file(xml_file=xml_file, path=path)
        moved_file = path + '/' + rename
        win32file.MoveFileWithProgress(xml_file, moved_file,
                                       None, None,
                                       0, None)
        return [moved_file, True]
    except Exception as ex:
        return [ex, False]

# Change status according to OMO_PULL_FREQUENCY
def change_status_is_completed(opta_xml_obj, opta_id):
    if not opta_id:
        try:
            opta_xml_obj.observation = opta_xml_obj.observation + 1
            opta_xml_obj.save()

            if opta_xml_obj.observation == settings.OMO_PULL_FREQUENCY:
                opta_xml_obj.is_completed = 1
                opta_xml_obj.save()
        except Exception as ex:
            logger.exception(ex)

# Creation of new OptaXmlUpdates record.
def create_optaxmlupdates_record(opta_xml_obj):
    try:
        opta_update_obj = OptaXmlUpdates(opta_xml=opta_xml_obj)
        opta_update_obj.save()
        logger.info('opta update record created')
        return opta_update_obj
    except Exception as ex:
        logger.exception(ex)

# Getting player names from f40 feed.
def get_players_name(players_feed_data):
    player_names = {}
    try:
        xmldoc = xml.dom.minidom.parse(players_feed_data)
        doc_root = xmldoc.documentElement
        if doc_root.getElementsByTagName('SoccerDocument'):
            #soccerdocument = doc_root.getElementsByTagName('SoccerDocument')
            team_tags = doc_root.getElementsByTagName('Team')
            for team in team_tags:
                #team_uid = team.getAttribute('uID')
                player_tags = team.getElementsByTagName('Player')

                for player in player_tags:
                    try:
                        player_uid = player.getAttribute('uID')
                        player_name = player.getElementsByTagName('Name')[0].firstChild.nodeValue
                        player_names[str(player_uid)] = player_name #str(player_name.encode('ascii', 'ignore'))
                    except Exception as ex:
                        logger.exception(ex)
                        continue
            return [True, player_names]
        else:
            logger.error('No player list exists on f40 feed for corresponding season_id & competition_id.')
            return [False, player_names]
    except Exception as ex:
        logger.exception(ex)
        return [False, player_names]

# Check basic configuration in settings file.
def check_settings_configuration():
    try:
        x = ''
        if settings.ROOT_FOLDER_PATH == x or settings.XML_WATCH_FOLDER_NAME == x or settings.EMAM_INGEST_FOLDER_FULL_PATH == x \
                or settings.ERROR_FOLDER_NAME == x or settings.PROCESSING_FOLDER_NAME == x:
            log_message = 'Folder configuration in settings file is not done properly. Please do it first to run the app.'
            return [False,log_message]
        elif settings.EMAM_USERNAME == x or settings.EMAM_PASSWORD == x or settings.UNIT_ID == x or settings.LICENSE_KEY == x \
                or settings.METADATA_GROUP_ID == x or settings.USER_KEY == x or settings.FILE_ACTION == x \
                or settings.SET_STANDARD_ID == x:
            log_message = 'eMAM Configuration Settings in settings file is not done properly. Please do it first to run the app.'
            return [False, log_message]
        elif settings.HOUSE_ID == x:
            log_message = 'HOUSE_ID not given in settings file. Please provide it first to run the app.'
            return [False, log_message]
        elif settings.OPTA_ID == x:
            log_message = 'OPTA_ID not given in settings file. Please provide it first to run the app.'
            return [False, log_message]
        elif settings.SOM_ID == x:
            log_message = 'SOM_ID not given in settings file. Please provide it first to run the app.'
            return [False, log_message]
        elif settings.SCORE_ID == x:
            log_message = 'SCORE_ID not given in settings file. Please provide it first to run the app.'
            return [False, log_message]
        elif not settings.VIDEO_FOLDER_NAMES.__len__():
            log_message = 'You need to provide atleast one video watch folder name to run the app.'
            return [False, log_message]
        log_message = 'Configuration seems good. Starting scheduler...'
        return [True, log_message]
    except Exception as ex:
        logger.exception(ex)

# Creating opta_xml in processing folder
def create_opta_xml(basic_xml_logs_obj):
    try:
        opta_file = settings.PATH_TO_PROCESSING_FOLDER + '/' + str(basic_xml_logs_obj.basic_xml.opta_id) + '.xml'
        file = open(opta_file, 'w')
        file.write(basic_xml_logs_obj.converted_xml)
        file.close()
        logger.info('[{0}] converted xml fetched and processing started.'.format(get_current_formatted_datetime()))
        log_message = "[{0}] Opta XML moved to processing folder.".format(get_current_formatted_datetime())
        logger.info(log_message)
        return [opta_file, log_message]
    except Exception as ex:
        logger.exception(ex)

def som_value(opta_xml):
    som_offset = '00:00:00:00'
    try:
        xmldoc = xml.dom.minidom.parse(opta_xml)
        doc_root = xmldoc.documentElement
        asset_tag = doc_root.getElementsByTagName('asset')[0]
        custom_metadata = asset_tag.getElementsByTagName('custom-metadata')[0]
        fields = custom_metadata.getElementsByTagName('field')
        for field in fields:
            if str(field.getAttribute("standard-id")) == settings.SOM_ID:
                som_offset = str(field.childNodes[0].data)
                return som_offset
        return som_offset
    except Exception as ex:
        logger.exception(ex)
        return som_offset