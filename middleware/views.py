# -*- coding: utf-8 -*-
from __future__ import unicode_literals
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.parsers import JSONParser
import logging
from models import BasicXml, BasicXmlLogs
from django.contrib.auth.decorators import login_required
from tasks import opta_updates
from django.shortcuts import render, redirect

logger = logging.getLogger(__name__)


# Create your views here.
class GetUploadNotifyView(APIView):
    """
    Receive house_id & asset_id from eMAM Director app on successful ingest.
    """
    #ToDo: Remember to add ingestion failure case handle.
    parser_classes = (JSONParser,)

    def post(self, request):
        house_id, asset_id = '', ''

        data = request.data

        if "house_id" in data:
            house_id = data["house_id"]

        if "asset_id" in data:
            asset_id = data["asset_id"]

        if asset_id and house_id:
            logger.info(house_id)
            logger.info(asset_id)
            try:
                xml_obj = BasicXml.objects.get(house_id=house_id)
                if xml_obj.asset_id == '':
                    xml_obj.asset_id = asset_id
                    xml_obj.save()
                else:
                    pass

            except Exception as ex:
                logger.info(ex)

        return Response({'message': None})

@login_required
def update_pull_request(request):
    #logger.error('helo')
    #logger.error(request.GET.get('opta_id'))
    opta_id = request.GET.get('opta_id')
    opta_updates(opta_id)

    return redirect(request.META.get('HTTP_REFERER'))