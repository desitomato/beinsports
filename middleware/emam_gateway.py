from enum import Enum

from suds.client import Client
from suds.sax.text import Raw
from suds.xsd.doctor import Import, ImportDoctor
from bein_sports import settings
from suds.sudsobject import asdict

class EMAMConstants:
    SORT_ASCENDING = 'ASC'
    SORT_DESCENDING = 'DESC'

    FILES_ALL = 0
    FILES_VIDEO = 1
    FILES_AUDIO = 2
    FILES_IMAGE = 3
    FILES_OTHER = 4

    ORDER_LAST_MODIFIED = 'lastmodified_on'
    ORDER_SIZE = 'size'
    ORDER_TITLE = 'title'
    ORDER_TYPE = 'type_name'
    ORDER_RATING = 'rating'
    ORDER_DESCRIPTION = 'asset_desc'
    ORDER_CATEGORY = 'category_name'
    ORDER_INGESTED_ON = 'ingested_on'
    ORDER_INGESTED_BY = 'ingested_by'


class MetadataField(Enum):
    """Metadata field types.
    """
    TEXT = 'Text'
    LIST = 'List'
    INT = 'Int'
    FLOAT = 'Float'
    DATETIME = 'DateTime'
    TIMECODE = 'TimeCode'
    MULTISELECT = 'MultiSelect'
    BUTTON = 'Button'


class MetadataOperation(Enum):
    """Metadata operations.
    """
    NONE = 'None'
    INSERT = 'Insert'
    UPDATE = 'Update'
    DELETE = 'Delete'


class MetadataSetType(Enum):
    """Metadata set type.
    """
    ALL = 'ALL'
    ASSET = 'Asset'
    PROJECT = 'Project'
    CATEGORY = 'Category'


class MetadataSet(Enum):
    """Metadata set types.
    """
    ASSET = 'Asset'
    PROJECT = 'Project'
    CATEGORY = 'Category'


class AssetType(Enum):
    """eMAM asset types.
    """
    VIDEO = '1'
    AUDIO = '2'
    IMAGE = '3'
    OTHER = '4'


class EMAMGateway:
    meta_ids = []

    def __init__(self, url='', name_space='http://tempuri.org/'):
        """Initialize the eMAM Gateway.
        :param url: Gateway URL
        :param name_space: NameSpace
        """
        self.url = url or settings.EMAM_GATEWAY_WSDL
        self.name_space = name_space
        imp = Import('http://www.w3.org/2001/XMLSchema', location='http://www.w3.org/2001/XMLSchema.xsd')
        imp.filter.add(name_space)
        doctor = ImportDoctor(imp)
        self.gateway_client = Client(self.url, doctor=doctor)

    def is_gateway_alive(self):
        """Check whether the gateway is alive or not.
        :return: True if alive
        """
        return self.gateway_client.service.IsGatewayAlive()

    def authenticate_user(self, user_name, password, unit_id, license_key):
        """Authenticate user with eMAM Gateway.
        :param user_name: Username
        :param password: Password
        :param unit_id: Unit ID
        :param license_key: License key
        :return: True if the authentication was successful
        """
        auth_obj = self.gateway_client.factory.create('clsSoapAuthentication')
        auth_obj.UserName = user_name
        auth_obj.Password = password
        auth_obj.UnitId = unit_id

        return self.gateway_client.service.AuthenticateUser(auth_obj, license_key)

    def get_search_assets(self, search_phrase='', project_id=0, category_id=0, asset_type=EMAMConstants.FILES_ALL,
                          order_by=EMAMConstants.ORDER_LAST_MODIFIED,
                          sort=EMAMConstants.SORT_ASCENDING):
        """Search assets in eMAM.
        :param search_phrase:
        :param project_id:
        :param category_id:
        :param asset_type:
        :param order_by:
        :param sort:
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        search_obj = self.gateway_client.factory.create('clsAssetSearchCriteria')
        search_obj.RefineCategory = 0
        search_obj.RefineStateID = 0
        search_obj.RefineIsArchived = False
        search_obj.RefineRating = 0.0
        search_obj.RefineIsBooleanSearch = False
        search_obj.ProjectID = 0
        search_obj.IsSorting = False
        search_obj.IsSearchClicked = False
        search_obj.IsAdvance = False
        search_obj.Category = 0
        search_obj.StateID = 0
        search_obj.UserID = 0
        search_obj.RoleID = 0
        search_obj.ClientID = 0
        search_obj.IsArchived = False
        search_obj.Rating = 0.0
        search_obj.IsBooleanSearch = False
        search_obj.MobilePlatformID = 0
        search_obj.FolderId = 0
        search_obj.IsOnline = False
        search_obj.IsFullTextSearch = False
        search_obj.SearchOption = 0
        search_obj.IncludeSubcategories = False
        search_obj.Searchphrase = search_phrase
        search_obj.SubProjectID = project_id
        search_obj.SubCategory = category_id
        search_obj.SubAssetType = asset_type
        search_obj.Order = sort
        search_obj.OrderBy = order_by
        search_obj.NoOfAssetPerPage = 100
        search_obj.CurrentPage = 1
        search_obj.SearchFilterId = 0
        search_obj.MINASSETSIZE = 0
        search_obj.MAXASSETSIZE = 0
        search_obj.IsFilterSearch = False
        search_obj.IsMarkerSearch = False
        search_obj.AssetDisplayType = 'List'

        return self.gateway_client.service.GetSearchAssets(search_obj, 1000)

    def get_custom_metadata(self, asset_id=0, unit_id=0):
        """Get custom metadata.
        :param asset_id: Asset ID, 0 to get all metadata
        :param unit_id: Unit ID, 0 to get default unit
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetCustomMetaData(asset_id, unit_id)

    def get_categories(self, check_assign_category_permission=True):
        """Get categories in eMAM.
        :param check_assign_category_permission: Check assign category permission
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetCategories(check_assign_category_permission)

    def get_projects(self, include_sub_project):
        """Get projects in eMAM.
        :param include_sub_project: Include sub projects with results
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        project_criteria = self.gateway_client.factory.create('clsProjectCriteria')
        project_criteria.UnitId = 0
        project_criteria.ProjectId = 0
        project_criteria.ProjectVersionId = 0
        project_criteria.IncludeSubProjects = include_sub_project
        project_criteria.IncludeAllVersions = False
        project_criteria.IncludeParentProject = True
        project_criteria.IncludeSequences = False
        project_criteria.IncludeUserList = False
        project_criteria.IncludeMetadata = False

        return self.gateway_client.service.GetProjects(project_criteria)

    def check_asset_permission(self, asset_id, permission_code, allowed):
        """Check asset permission.
        :param asset_id: Asset ID
        :param permission_code: Permission Code
        :param allowed: Is allowed
        :return: True, if permission allowed
        """
        return self.gateway_client.service.CheckAssetPermission(asset_id, permission_code, allowed)

    def get_unit_ingest_profiles(self, unit_id, user_id, ingest_profile_id):
        """Get ingest profiles in eMAM.
        :param unit_id: Unit ID
        :param user_id: User ID
        :param ingest_profile_id: Ingest profile ID
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetUnitIngestProfiles(unit_id, user_id, ingest_profile_id)

    def add_ingest_queue_details(self, unit_id, ingest_folder_id, transcoder, ingest_queue_title, status,
                                 create_no_proxy, rename_duplicate,
                                 is_proxy, source_path, managed_computer_id, file_action, is_media_content,
                                 sequence_type, tags, author,
                                 description, custom_metadatas, projects, categories, markers, platform_assets,
                                 metadata_set_id,
                                 configuration, checksum, priority, parent_asset_id, ingest_action, asset_title):
        """Add a new ingest queue entry in eMAM.
        :param unit_id: eMAM unit ID
        :param ingest_folder_id: Ingest folder ID
        :param transcoder: Selected transcoder
        :param ingest_queue_title: Ingest queue title
        :param status: Ingest status
        :param create_no_proxy: Create proxy
        :param rename_duplicate: Rename if duplicate
        :param is_proxy: Is ingesting proxy file
        :param source_path: Source file path
        :param managed_computer_id: Source computer's managed computer ID
        :param file_action: Source file action
        :param is_media_content: Is media content
        :param sequence_type: Sequence type
        :param tags: Tags
        :param author: Author
        :param description: Description
        :param custom_metadatas: Custom metadatas
        :param projects: Projects
        :param categories: Categories
        :param markers: Markers
        :param platform_assets: Extra platfrom assets
        :param metadata_set_id: Custom metadata set ID
        :param configuration: Server configuration
        :param checksum: Source file checksum
        :param priority: Ingest priority
        :param parent_asset_id: Parent asset ID
        :param ingest_action: Ingest action
        :param asset_title: Asset title
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.AddIngestQueueDetails(unit_id, ingest_folder_id, transcoder,
                                                                 ingest_queue_title, status, create_no_proxy,
                                                                 rename_duplicate, is_proxy, source_path,
                                                                 managed_computer_id, file_action, is_media_content,
                                                                 sequence_type, tags, author, description,
                                                                 Raw(custom_metadatas), Raw(projects), Raw(categories),
                                                                 markers, platform_assets, metadata_set_id,
                                                                 configuration, None, checksum, priority,
                                                                 parent_asset_id, ingest_action, asset_title)

    def ingest_new_asset(self, uuid, ingest_queue_id, storage_profile_id):
        """Index new asset with eMAM system.
        :param uuid:
        :param ingest_queue_id:
        :param storage_profile_id:
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        file_info = self.gateway_client.factory.create('clsAssetFileInfo')
        file_info.UnitId = -1
        file_info.IngestedBy = 0
        file_info.ParentAssetId = -1
        file_info.ParentAssetVersionId = -1
        file_info.CategoryID = -1
        file_info.ProjectID = -1
        file_info.UserID = 0
        file_info.ProfileID = -1
        file_info.Size = 0
        file_info.BitRate = '0.0'
        file_info.FrameRate = '0.0'
        file_info.HasKeyFrame = False
        file_info.Title = ''
        file_info.AssetType = 'Video'
        file_info.AssetExtention = ''
        file_info.AssetFileName = ''
        file_info.AssetDesc = ''
        file_info.Author = ''
        file_info.Action = 'INGT'
        file_info.Duration = ''
        file_info.Resolution = ''
        file_info.UUID = uuid
        file_info.Subject = ''
        file_info.Category = ''
        file_info.Copyright = ''
        file_info.Keywords = ''
        file_info.Location = ''
        file_info.FolderPath = ''
        file_info.OtherPlatformFileNames = ''
        file_info.DateCreated = '2016-09-21T13:52:37.151'
        file_info.MachineId = ''
        file_info.CreateCategoryFromFolder = True
        file_info.IngestQueueId = ingest_queue_id
        file_info.IsDuplicateRestricted = True
        file_info.RenameFile = False
        file_info.TimeOffset = ''

        return self.gateway_client.service.IngestNewAsset(None, file_info, None, None, storage_profile_id, None)

    def update_ingest_queue_details(self, ingest_queue_id, ingest_folder_id, uuid, status, description, create_no_proxy,
                                    rename_duplicate):
        """Update ingest queue details.
        :param ingest_queue_id: Ingest queue ID
        :param ingest_folder_id: Ingest folder ID
        :param uuid: Asset UUID
        :param status: Ingest status
        :param description: Status description
        :param create_no_proxy: Create no proxy
        :param rename_duplicate: Rename duplicate
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.UpdateIngestQueueDetails(None, ingest_queue_id, ingest_folder_id, uuid,
                                                                    status, description,
                                                                    None, create_no_proxy, rename_duplicate)

    def get_workflow(self, client_id, workflow_id):
        """Get workflow in eMAM.
        :param client_id: Client ID
        :param workflow_id: eMAM workflow ID
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetWorkflow(client_id, workflow_id)

    def update_metadata_field(self, field_id, description, field_type, field_name, default_value, required, visible,
                              asset_types, list_values, operation_type):
        """Update metadata field in eMAM.
        :param field_id: Metadata field ID
        :param description: Field description
        :param field_type: Field type
        :param field_name: Field name
        :param default_value: Default value
        :param required: Is this filed required
        :param visible: Field visibility
        :param asset_types: Asset types
        :param list_values: List field values
        :param operation_type: Metadata field operation
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        metadata_field = self.gateway_client.factory.create('MetadataFieldConfig')
        metadata_field.MetadataFieldId = field_id
        metadata_field.MetadataFieldDescription = description
        metadata_field.MetadataFieldType = field_type
        metadata_field.MetadataFieldName = field_name
        metadata_field.DefaultFieldValue = default_value
        metadata_field.IsRequired = required
        metadata_field.Visibility = visible
        metadata_field.AssetTypes = asset_types
        list_items = self.gateway_client.factory.create('ArrayOfListFields')
        list_items.ListFields = []
        for value in list_values:
            list_field = self.gateway_client.factory.create('ListFields')
            list_field.Value = value
            list_items.ListFields.append(list_field)
        metadata_field.ListItems = list_items
        metadata_field.OperationType = operation_type
        metadata_field.MetadataFieldLength = 0

        return self.gateway_client.service.UpdateMetadataField(metadata_field)

    def update_metadata_group(self, group_id, group_name, description, operation_type, view_permission,
                              update_permission, list_fields):
        """Update metadata group in eMAM.
        :param group_id: Group ID
        :param group_name: Group name
        :param description: Description
        :param operation_type: Operation type
        :param view_permission: View permission
        :param update_permission: Update permission
        :param list_fields: List of fields
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        metadata_group = self.gateway_client.factory.create('MetadataGroupConfig')
        metadata_group.MetadataGroupId = group_id
        metadata_group.MetadataGroupName = group_name
        metadata_group.Description = description

        metadata_group.MetadataFieldIds = """<xs:schema id="NewDataSet" xmlns="" 
            xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">
        <xs:element name="NewDataSet" msdata:IsDataSet="true" msdata:MainDataTable="UploadTable"
            msdata:UseCurrentLocale="true">
        <xs:complexType>
        <xs:choice minOccurs="0" maxOccurs="unbounded">
        <xs:element name="UploadTable">
        <xs:complexType>
        <xs:sequence>
        <xs:element name="MetadataFieldId" type="xs:string" minOccurs="0" />
        <xs:element name="MetadataFieldOrder" type="xs:string" minOccurs="0" />
        <xs:element name="Include" type="xs:string" minOccurs="0" />
        </xs:sequence>
        </xs:complexType>
        </xs:element>
        </xs:choice>
        </xs:complexType>
        </xs:element>
        </xs:schema>
        <diffgr:diffgram xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" 
            xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1"><DocumentElement xmlns="">"""
        for index, field in enumerate(list_fields):
            metadata_group.MetadataFieldIds += """<UploadTable diffgr:id="UploadTable{}" msdata:rowOrder="{}"
                diffgr:hasChanges="inserted">
            <MetadataFieldId>{}</MetadataFieldId>
            <MetadataFieldOrder>{}</MetadataFieldOrder>
            <Include>{}</Include>
            </UploadTable>""".format(index + 1, index, field, 0, True)
        metadata_group.MetadataFieldIds += """</DocumentElement>
        </diffgr:diffgram>"""
        metadata_group.MetadataFieldIds = Raw(metadata_group.MetadataFieldIds)

        metadata_group.OperationType = operation_type
        metadata_group.ViewPermission = view_permission
        metadata_group.UpdatePermission = update_permission

        return self.gateway_client.service.UpdateMetadataGroup(metadata_group)

    def update_metadata_set(self, set_id, set_name, description, operation_type, set_type, list_groups):
        """Update metadata set in eMAM.
        :param set_id: Metadata set ID
        :param set_name: Set name
        :param description: description
        :param operation_type: Operation type
        :param set_type: Set type
        :param list_groups: List of groups
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        metadata_set = self.gateway_client.factory.create('MetadataSetConfig')
        metadata_set.MetadataSetId = set_id
        metadata_set.MetadataSetName = set_name
        metadata_set.Description = description
        metadata_set.OperationType = operation_type.value
        metadata_set.MetadataSetType = set_type.value

        metadata_set.MetadataGroupIds = """<xs:schema id="NewDataSet" xmlns="" xmlns:xs="http://www.w3.org/2001/XMLSchema" 
            xmlns:msdata="urn:schemas-microsoft-com:xml-msdata">
        <xs:element name="NewDataSet" msdata:IsDataSet="true" msdata:MainDataTable="UploadTable" 
            msdata:UseCurrentLocale="true">
        <xs:complexType>
        <xs:choice minOccurs="0" maxOccurs="unbounded">
        <xs:element name="UploadTable">
        <xs:complexType>
        <xs:sequence>
        <xs:element name="MetadataGroupId" type="xs:string" minOccurs="0" />
        <xs:element name="MetadataGroupOrder" type="xs:string" minOccurs="0" />
        <xs:element name="Include" type="xs:string" minOccurs="0" />
        </xs:sequence>
        </xs:complexType>
        </xs:element>
        </xs:choice>
        </xs:complexType>
        </xs:element>
        </xs:schema>
        <diffgr:diffgram xmlns:msdata="urn:schemas-microsoft-com:xml-msdata" 
            xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
        <DocumentElement xmlns="">"""
        for index, group in enumerate(list_groups):
            metadata_set.MetadataGroupIds += """<UploadTable diffgr:id="UploadTable{}" msdata:rowOrder="{}" 
                diffgr:hasChanges="inserted">
            <MetadataGroupId>{}</MetadataGroupId>
            <MetadataGroupOrder>{}</MetadataGroupOrder>
            <Include>{}</Include>
            </UploadTable>""".format(index + 1, index, group[0], group[1], True)
        metadata_set.MetadataGroupIds += """</DocumentElement>
        </diffgr:diffgram>"""
        metadata_set.MetadataGroupIds = Raw(metadata_set.MetadataGroupIds)

        return self.gateway_client.service.UpdateMetadataSet(metadata_set)

    def get_metadata_fields(self, field_id=0, search_phrase=''):
        """Get metadata fields in eMAM.
        :param field_id: Metadata field ID
        :param search_phrase: Search phrase
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetMetadataFields(field_id, search_phrase)

    def get_metadata_groups(self, group_id=0, include_group_fields=False, search_phrase='',
                            check_view_permission=False):
        """Get metadata groups in eMAM.
        :param group_id: Metadata group ID
        :param include_group_fields: Include metadata fields in group
        :param search_phrase: Search phrase
        :param check_view_permission: Check view permission
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetMetadataGroups(group_id, include_group_fields, search_phrase,
                                                             check_view_permission)

    def get_metadata_sets(self, set_id=0, metadata_filter=MetadataSetType.ALL, include_groups=False, search_phrase=''):
        """Get metadata sets in eMAM.
        :param set_id: Metadata set ID
        :param metadata_filter: Metadata filter
        :param include_groups: Include groups
        :param search_phrase: Search phrase
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        return self.gateway_client.service.GetMetadataSets(set_id, metadata_filter.value, include_groups, search_phrase)

    def update_asset_metadata(self, asset_id, metadata_set_id, metadata_values, category_id=0, project_id=0,
                              project_version_id=0, author=''):
        """Update asset metadata.
        :param asset_id: Asset ID
        :param category_id: Category ID
        :param project_id: Project ID
        :param project_version_id: Project version ID
        :param author: Author
        :param metadata_set_id: Metadata set ID
        :param metadata_values: List of metadata field, value tuples
        :return: eMAM Gateway response, ResponseId > 0 if success
        """
        metadata_details = self.gateway_client.factory.create('MetadataConfig')
        metadata_details.AssetId = asset_id
        metadata_details.CategoryId = category_id
        metadata_details.ProjectId = project_id
        metadata_details.ProjectVersionId = project_version_id
        metadata_details.Author = author
        metadata_details.MetadataSetId = metadata_set_id
        metadata_details.MetadataValues = self.gateway_client.factory.create('ArrayOfClsMetadataValue')
        metadata_details.MetadataValues.clsMetadataValue = []
        for metadata in metadata_values:
            field = self.gateway_client.factory.create('clsMetadataValue')
            field.FieldId = metadata[0]
            field.FieldValue = metadata[1]
            metadata_details.MetadataValues.clsMetadataValue.append(field)

        return self.gateway_client.service.UpdateAssetMetaData(metadata_details)

    def recursive_dict(self, sudsobject):
        """ Get specific key value by iterating through the object in the form of dictionary
        returned by suds as soap XML response

        :param sudsobject:
        :return:
        """
        out = {}
        for k, v in asdict(sudsobject).iteritems():
            if hasattr(v, '__keylist__'):
                out[k] = self.recursive_dict(v)
            elif isinstance(v, list):
                out[k] = []
                for item in v:
                    if hasattr(item, '__keylist__'):
                        data = self.recursive_dict(item)
                        try:
                            if 'METADATA_FIELD_ID' in data:
                                self.meta_ids.append(str(data['METADATA_FIELD_ID']))
                        except Exception:
                            pass
                        out[k].append(data)
                    else:
                        out[k].append(item)
            else:
                out[k] = v
        return out
