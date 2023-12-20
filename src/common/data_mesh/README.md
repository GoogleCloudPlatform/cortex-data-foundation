# Data Mesh User Guide

## Overview

Data Mesh for Cortex extends the data foundation to enable data governance,
discoverability, and access control through BigQuery metadata and
[Dataplex](https://cloud.google.com/dataplex). This is implemented by providing
a base set of metadata resources and BigQuery asset annotations that can be
customized and optionally deployed alongside the data foundation. These base
specifications provide an opinionated configuration that are the metadata
foundation to complement Cortex Data Foundation.

![Data Mesh logical schema](data_mesh1.png)

This is an experimental component that will change over time. Reach out
to [cortex-support@google.com](mailto:cortex-support@google.com) with feedback
on existing or requested features.

## Concepts

This section will help explain how relevant Data Mesh concepts are applied
within Cortex. Be familiar with this section before proceeding with the rest of
this guide. For a deeper understanding of general Data Mesh concepts, read
[Build a modern, distributed Data Mesh with Google Cloud](https://services.google.com/fh/files/misc/build-a-modern-distributed-datamesh-with-google-cloud-whitepaper.pdf).

Term                                                                                                        | GCP Product                                                                     | Description                                                                                                                                                                                     | Cortex Application
----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------
[Lake](https://cloud.google.com/dataplex/docs/introduction#terminology)                                     | [Dataplex > Manage Lakes](https://console.cloud.google.com/dataplex/lakes)      | Top level unit for organizing data within a Data Mesh.                                                                                                                                          | A data source, e.g. `SAP ECC`, `Salesforce`, `Google Ads`.
Zone                                                                                                        | Dataplex                                                                        | Second level unit for organizing data within a Lake.                                                                                                                                            | Specific processing layers within a data source, like raw vs CDC.
Dataplex Asset                                                                                              | Dataplex                                                                        | Reference to data that is stored in Cloud Storage or BigQuery that is associated with a zone. This is a reference to the data asset and not the data itself.                                    | Reference to BigQuery datasets registered in zones. In the future these will be tables or views.
Label                                                                                                       | Dataplex                                                                        | Arbitrary key value pairs that can be applied to lakes or zones.                                                                                                                                | Label entire lakes or zones (rather than tables or columns) with metadata that can be viewed in Dataplex or used for custom applications.
[Data Catalog](https://cloud.google.com/data-catalog/docs/concepts/overview)                                | Dataplex                                                                        | Technical business metadata that can be used to help discover, understand, or manage data assets within a warehouse.                                                                            | Annotate tables or columns (rather than lakes or zones) with rich metadata tags that can be used in Dataplex search or custom applications.
[Catalog Tag Templates](https://cloud.google.com/data-catalog/docs/tags-and-tag-templates)                  | [Dataplex > Tag Templates](https://console.cloud.google.com/dataplex/templates) | A template defining the available fields and their types in a tag.                                                                                                                              | Define a set of templates for uses like tagging data assets with lines of business.
Catalog Tag                                                                                                 | Dataplex                                                                        | A set of fields and their values that contain metadata applicable to a table or column. An instance of a tag template.                                                                          | Annotate a table or column with metadata values relevant to that asset, such as a particular line of business.
[Catalog Glossary](https://cloud.google.com/dataplex/docs/create-glossary)                                  | [Dataplex > Glossaries](https://console.cloud.google.com/dataplex/glossaries)   | A dictionary of terms that can be defined and associated with BigQuery columns.                                                                                                                 | Define terms or acronyms used in BigQuery Assets. Note that this is planned for the future and is not currently supported.
[Policy Taxonomy](https://cloud.google.com/bigquery/docs/column-level-security-intro)                       | [BigQuery > Policy Tags](https://console.cloud.google.com/bigquery/policy-tags) | A hierarchy of policy tags.                                                                                                                                                                     | Organize related policy tags that can be used for access control into a hierarchy with [inherited permissions](https://cloud.google.com/bigquery/docs/column-data-masking-intro#auth-inheritance).
Policy Tag                                                                                                  | BigQuery                                                                        | A tag that is applied to specific columns within a Bigquery table or view. Policy tags at any level in the hierarchy can be applied. Only one policy tag can be applied to a particular column. | Annotate columns with tags that are used for column-level access control. Principals on the policy tag define 'Fine-Grained' or 'Unmasked' Readers who can see the raw column data.
Data Policy                                                                                                 | BigQuery                                                                        | Policies applied to a Policy Tag that define how and who can view the [masked column data](https://cloud.google.com/bigquery/docs/column-data-masking-intro).                                   | Principals on the Data Policy define the 'Masked readers' who can see the masked column data. Anyone who doesn't have masked or unmasked reader privileges will not be able to query the column.
Masking Rule                                                                                                | BigQuery                                                                        | Rules applied to a Data Policy that define how the data is masked, e.g. hashing, showing a default value, last 4 chars, etc.                                                                    | Applied situationally to sensitive columns.
[Row Access Policy](https://cloud.google.com/bigquery/docs/row-level-security-intro)                        | BigQuery                                                                        | SQL statements that define which groups can query rows within tables based on specific column values.                                                                                           | Used for row-level access control when asset and column level control is insufficient.
Metadata Resource                                                                                           | Cortex Data Mesh term                                                           | All of the metadata objects listed above, which are used in the data mesh. This is specifically the metadata and not the data in BigQuery itself.                                               | The entirety of these resources compose the Cortex Data Mesh.
BigQuery Asset                                                                                              | Cortex Data Mesh term                                                           | BigQuery table or view.                                                                                                                                                                         | Existing Cortex BigQuery objects that we would like to govern with the Data Mesh.
BigQuery Asset Annotation                                                                                   | Cortex Data Mesh term                                                           | Metadata resources applied to a specific BigQuery table or view.                                                                                                                                | Associate metadata with BigQuery Assets to enable discovery and access control.
Resource Specification (spec)                                                                               | Cortex Data Mesh term                                                           | A YAML file defining a Metadata Resource or BigQuery Asset Annotation.                                                                                                                          | The full set of resource specs codifies the Data Mesh configuration to be deployed.
[Data Lineage](https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage)                      | Dataplex                                                                        | A graph representing BigQuery Asset dependencies.                                                                                                                                               | These are not defined by the Cortex Data Mesh, however it is a relevant Dataplex tool to help users discover BigQuery Asset data sources.
[Lineage Event](https://cloud.google.com/data-catalog/docs/concepts/about-data-lineage#lineage-model-event) | Dataplex                                                                        | A point in time when an operation occurred to move data between BigQuery Assets. Contains a list of Links.                                                                                      | Automatically created for supported BigQuery and Composer operations.
Lineage Link                                                                                                | Dataplex                                                                        | An edge representing data flowing from a source to target asset as part of a Lineage Event.                                                                                                     | Can be analyzed to support use cases beyond the lineage visualization graphs that are presented in the console.

## Design

Data Mesh for Cortex is designed similarly to the overall data foundation and
can be thought of in three phases with various subcomponents handled by Cortex
or users.

1.  With each release, Cortex will update the base resource specs that provide
    the opinionated metadata foundation for the Data Mesh.
2.  Before deployment, users can customize the resource specs to fit their use
    case and needs.
3.  Users can enable data mesh in the Cortex config, and it will be deployed
    after the data assets during the Cortex deployment. Users can also deploy
    the data mesh independently to make further updates.

## Enable APIs and Verify Permissions

Follow the documentation sections referenced to enable APIs and verify
permissions for any Data Mesh features that will be used. The permissions will
need to be granted for any user deploying the Data Mesh or the Cloud Build
account if deployed with the data foundation. If the deployment uses different
source and target projects, these APIs and roles will need to be enabled in
both projects wherever those features are used.

Feature                                                                                                                                                                                                           | Roles                          | Notes
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------ | -----
BigQuery [asset](https://cloud.google.com/bigquery/docs/control-access-to-resources-iam#required_roles) and [row](https://cloud.google.com/bigquery/docs/managing-row-level-security#required_permissions) access | BigQuery Data Owner            |
BigQuery [column access](https://cloud.google.com/bigquery/docs/column-level-security#before_you_begin)                                                                                                           | Policy Tag Admin               | More info on [IAM roles](https://cloud.google.com/bigquery/docs/column-level-security-intro#roles)
[Catalog Tags](https://cloud.google.com/data-catalog/docs/tag-bigquery-dataset#before-you-begin)                                                                                                                  | Data Catalog TagTemplate Owner | More info on [IAM roles](https://cloud.google.com/data-catalog/docs/concepts/iam)
[Dataplex Lakes](https://cloud.google.com/dataplex/docs/create-lake#before-you-begin)                                                                                                                             | Dataplex Editor                | Dataproc and Dataproc Metastore APIs are __NOT__ needed for Cortex Data Mesh.

## Understanding the Base Resource Specs

The primary interface for configuring the Data Mesh for Cortex is through the base
resource specs, which are a set of YAML files provided out of the box that
define the metadata resources and annotations that will be deployed. The base
specs provide initial recommendations and syntax examples, but are intended to
be customized further to suit user needs. These specs fall into two categories:

*   **Metadata Resources** that can be applied across various data assets, e.g.
    Catalog Tag Templates that define how assets can be tagged with business
    domains.
*   **Annotations** that specify how the metadata resources are applied to a
    particular data asset, e.g. a Catalog Tag that associates a specific table
    to the Sales domain.

The following sections will step through basic examples of each spec type and
explain how to customize them. The base specs are marked with
`## CORTEX-CUSTOMER` where they should be modified to fit a deployment if the
associated [deployment option](#deployment-options) is enabled. For advanced
uses, see the canonical definition of these spec schemas found in
[`src/common/data_mesh/src/data_mesh_types.py`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py).

### Metadata Resources {#metadata-resources}

These resources are shared entities that exist within a project that can be
applied to many data assets. Most of the specs include a `display_name` field
subject to the following requirements:

*   contains only Unicode letters, numbers (0-9), underscores (_), dashes (-),
    and spaces ( )
*   can't start or end with spaces
*   maximum length is 200 characters

In some cases the `display_name` is also used as an ID, which may introduce
additional requirements. In those cases links to canonical documentation will be
included below.

If the deployment references metadata resources in different source and target
projects, there must be a spec defined for each project. For
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SFDC/config/lakes),
there are two Lake specs for SFDC. One for the raw and CDC zones, and one for
reporting.

#### Dataplex Lakes, Zones, and Assets

Dataplex Lakes, Zones, and Assets are used to organize the data from an
engineering perspective.

Lakes have a `region` and zones have a `location_type`, both of these are
related to the Cortex location (`config.json` > `location`). The Cortex
location defines where the Bigquery Datasets will be stored and can be a single
or multi-region. The zone `location_type` should be set to
`SINGLE_REGION | MULTI_REGION` to match that. However Lake regions must always
be a single region. If the Cortex location and zone `location_type` are
multi-region, select a single region within that group for the Lake region.

##### Requirements

*   The lake `display_name` will be used as the `lake_id` and must comply with
    those
    [requirements](https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.services.dataplex_service.DataplexServiceClient#google_cloud_dataplex_v1_services_dataplex_service_DataplexServiceClient_create_lake).
    This is also the case with the
    [zone](https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.services.dataplex_service.DataplexServiceClient#google_cloud_dataplex_v1_services_dataplex_service_DataplexServiceClient_create_zone)
    and
    [asset](https://cloud.google.com/python/docs/reference/dataplex/latest/google.cloud.dataplex_v1.services.dataplex_service.DataplexServiceClient#google_cloud_dataplex_v1_services_dataplex_service_DataplexServiceClient_create_asset)
    `display_name`s. Note that zone IDs must be unique across all Lakes in the project.
*   Lake specs must be associated with a single region.
*   An `asset_name` which matches a BQ dataset ID. This is different from the
    asset `display_name` which is how the asset is shown in Dataplex.

##### Limitations

*   Dataplex currently only supports registration of BigQuery datasets rather
    than individual tables as Dataplex assets.
*   An asset may only be registered in a single zone.
*   Dataplex is only supported in certain
    [locations](https://cloud.google.com/dataplex/docs/locations).

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/lakes/lakes.yaml).

These resources are defined in YAML files that specify
[`data_mesh_types.Lakes`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L258).

#### Catalog Tag Templates {#catalog-tag-templates}

Data Catalog Tag Templates can be used to add context to BigQuery tables or
individual columns. One of their uses in Cortex is to organize the data from a
business perspective (i.e. line of business) in a way that is integrated with
Dataplex search tooling. Templates are only the definition of which fields exist
in a particular tag and the field types. [Catalog Tags](#catalog-tags) are
instances of the templates with actual field values.

Template field `display_name` s are used as the field ID and must follow the
[requirements](https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.TagTemplate)
listed for `TagTemplate.fields` .

See
[here](https://cloud.google.com/python/docs/reference/datacatalog/latest/google.cloud.datacatalog_v1.types.FieldType)
for a list of supported field types.

Cortex Data Mesh will create all tag templates as publicly readable. It also
introduces an additional `level` concept to tag template specs, which specifies
the grain at which the template should be applied with the possible values:
`ASSET | FIELD | ANY`. This is not currently enforced, but in the future we may
enable validation checks during data mesh deployment that tags aren't being
applied at an inappropriate level.

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/tag_templates/templates.yaml).

Templates are defined in YAML files that specify
[`data_mesh_types.CatalogTagTemplates`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L123).

 Catalog Tags are instances of the templates, and are discussed below within the
[Asset Annotations](#catalog-tags).

#### Asset and Column Level Access Control with Tag Templates

Cortex provides the ability to enable [asset](#asset-level-access) or
[column](#column-level-access) level access control on all artifacts that are
associated with a Catalog Tag Template. For example, if users would like to
grant access to assets based on line of business, they can create
`asset_policies` for the `line_of_business` Catalog Tag Template with different
principals specified for each business domain. Each policy accepts `filters`
that can be used to only match tags with specific values. In this case we could
match the `domain` values. Note that these `filters` currently only support
matching for equality and no other operators. If multiple filters are listed,
the results must satisfy all filters (i.e. `filter_a AND filter_b` ). The final
set of asset policies is the union of those defined directly in the annotations,
and those from the template policies.

Column level access control with Catalog Tags behaves similarly by applying
[Policy Tags](#policy-taxonomies-and-tags) on matching fields. However, because
only one Policy Tag can be applied to a column, the precedence is:

1.  Policy Tag defined directly in the annotation spec
1.  First matching Catalog Tag Template field policy

If this feature is used, we highly recommend enabling or disabling the
[deployment](#deployment-options) of Catalog and ACLs together.

To understand the specs for this advanced feature, view the definitions for
[`data_mesh_types.CatalogTagTemplate`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L104)
`asset_policies` and `field_policies`.

#### Catalog Glossary

The glossary is a tool that can be used to provide a dictionary of terms used by
specific columns within data assets that may not be universally understood.
Currently users can add terms manually in the console, but there is no support
through the resource specs.

#### Policy Taxonomies & Tags {#policy-taxonomies-and-tags}

Policy taxonomies and tags allow
[column level access control](#column-level-access) over sensitive data assets
in a standardized way. For example, there could be a taxonomy for tags
controlling PII data on a particular line of business, where only certain groups
can read masked data, unmasked data, or have no read access at all.

Read this
[doc](https://cloud.google.com/bigquery/docs/column-data-masking-intro) for
more details, the following sections are particularly relevant:

*   [How Masked vs Fine-Grained Reader roles interact](https://cloud.google.com/bigquery/docs/column-data-masking-intro#role-interaction)
*   [Authorization inheritance](https://cloud.google.com/bigquery/docs/column-data-masking-intro#auth-inheritance).
*   [Masking rules](https://cloud.google.com/bigquery/docs/reference/bigquerydatapolicy/rest/v1/projects.locations.dataPolicies?&_ga=2.38828065.-309650094.1698781951#predefinedexpression)
    and
    [hierarchy](https://cloud.google.com/bigquery/docs/column-data-masking-intro#data_masking_rule_hierarchy).

Also read about
[policy tag best practices](https://cloud.google.com/bigquery/docs/best-practices-policy-tags).

Cortex provides sample policy tags to demonstrate how they are specified and
potential uses, however resources that affect access control are not enabled in
the Data Mesh deployment by default.

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/policy_taxonomies/policy_taxonomies.yaml).

Policy Taxonomies are defined in YAML files that specify
[`data_mesh_types.PolicyTaxonomies`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L180).

### Asset Annotations {#asset-annotations}

Annotations specify metadata applicable to a particular asset and may reference
the shared metadata resources that were defined above.

Annotations include:
*   asset descriptions
*   field descriptions
*   Catalog Tags
*   Asset, row, and column level access control

Note: The console currently renders new lines in descriptions as a
whitespace.

Currently, base annotation descriptions are provided for the workloads listed
below. Descriptions for other sources will be added over time.

*   SAP ECC (raw, CDC, and reporting)
*   SAP S4 (raw, CDC, and reporting)
*   SFDC (reporting only)
*   Marketing CM360 (reporting only)
*   Marketing GoogleAds (reporting only)
*   Marketing TikTok (reporting only)

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/annotations/BalanceSheet.yaml).

Annotations are defined in YAML files that specify
[`data_mesh_types.BqAssetAnnotation`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L309).

#### Catalog Tags {#catalog-tags}

Catalog Tags are instances of the defined [templates](#catalog-tag-templates)
where field values are assigned that apply to the specific asset. Be sure to
assign values that match the field types declared in the associated template.

`TIMESTAMP` values should be in one of the following
[formats](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes):

```
"%Y-%m-%d %H:%M:%S%z"
"%Y-%m-%d %H:%M:%S"
"%Y-%m-%d"
```

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/annotations/BalanceSheet.yaml#L3).

Spec definition:
[`data_mesh_types.CatalogTag`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L117).

#### Specifying Access Policy Readers & Principals

When specifying readers and principals for access policies at any of the
following levels, the strings must be valid principals following the format of
[IAM Policy Binding member](https://cloud.google.com/iam/docs/reference/rest/v1/Policy#binding).

#### Asset Level Access Control {#asset-level-access}

Access can be granted on individual BigQuery assets with `READER`, `WRITER`, or
`OWNER` permissions. These permissions are equivalent to the
[`GRANT` DCL statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-control-language#grant_statement).

Unlike the behavior for most resources and annotations, the
[overwrite](#overwrite) flag will not remove existing principals with the
`OWNERS` role. When new owners are specified with overwrite enable, they will
only be appended to the existing owners. This is a safeguard to prevent
unintended loss of access. To remove asset owners, use the console.
Overwriting will remove existing principals with the `READER` or `WRITER` role.

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/annotations/BalanceSheet.yaml#L62).

Spec definition:
[`data_mesh_types.BqAssetPolicy`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L288).

#### Row Level Access Control {#row-level-access}

Access can also be granted on sets of rows based on certain column value
filters. When specifying the row access policy, the provided filter string will
be inserted into a
[`CREATE` DDL statement](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_row_access_policy_statement).
By default, this uses `CREATE ROW ACCESS POLICY IF NOT EXISTS`. If
[overwrite](#overwrite) is enabled, this drops all existing row policies
before deploying new ones.

Note:

*   Adding any row access policies means that any users not specified in those
    policies will no longer have access to see any rows.
*   Row policies are only supported on tables and not views.
*   Avoid having row level policies filter on a partitioned column. See the
    associated reporting settings YAML file for information on the asset type
    and partitioned columns.

See here for more
[row level security best practices](https://cloud.google.com/bigquery/docs/best-practices-row-level-security).

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/annotations/BalanceSheet.yaml#L67).

Spec definition:
[`data_mesh_types.BqRowPolicy`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L273).

#### Column Level Access Control {#column-level-access}

To enable column level access, annotate individual fields with a
[Policy Tag](#policy-taxonomies-and-tags) identified by the Policy Tag name and
Taxonomy name. Update the policy tag metadata resource to configure access
control.

See the following
[example](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/SAP/SAP_REPORTING/config/ecc/annotations/SalesOrderDetails_SAMPLE.yaml#L13).

Spec definition:
[`data_mesh_types.PolicyTagId`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/src/data_mesh_types.py#L154).

### Spec Directories {#spec-directories}

The base specs are in config directories similar to the following locations:

Base Spec Granularity | Description                             | Directory Path
--------------------- | --------------------------------------- | --------------
Data source           | Specs for a particular data source      | `src/<workload>/src/<data_source>/config/<spec_type>/`<br><br>`src/marketing/src/CM360/config/lakes/`
Asset                 | Specs that apply to a single data asset | `src/<workload>/src/<data_source>/config/<spec_type>/<layer>/`<br><br>`src/marketing/src/CM360/config/annotations/reporting/`

Note: Paths may be slightly different to account for each workload's unique file
structure, but they will be found similarly under `config`.

[Metadata Resources](#metadata-resources) are defined at the data source level
with a single YAML file in the directory containing a list of all the
resources. Users can extend the existing file or create additional YAML files
containing additional resource specs within that directory if desired.

[Asset Annotations](#asset-annotations) are defined at the asset level and
contain many YAML files in the directory with a single annotation per file.

## Deploying the Data Mesh {#deployment}

The Data Mesh can either be deployed as part of the data foundation deployment,
or on its own. In either case, it will use the Cortex `config.json` file to
determine relevant variables, such as BigQuery dataset names and deployment
options. By default, deploying the Data Mesh will not remove or overwrite any
existing resources or annotations to prevent any unintentional losses. However,
there is also an ability to [overwrite](#overwrite) existing resources when
deployed on its own.

### Deployment Options {#deployment-options}

The following deployment options can be enabled or disabled based on the user's
needs and spend constraints in `config.json` > `DataMesh`.

| Option               | Notes                                                 |
| -------------------- | ----------------------------------------------------- |
| `deployDescriptions` | This is the only option enabled by default and will deploy BigQuery annotations with asset and column descriptions. It doesn't require enabling any additional APIs or permissions. |
| `deployLakes`        | Deploys Lakes and Zones                               |
| `deployCatalog`      | Deploys Catalog Template resources and their associated Tags in asset annotations. |
| `deployACLs`         | Deploys Policy Taxonomy resources and asset, row, and column level access control policies through asset annotations. The logs will contain messages indicating how the access policies have changed. |

> [!IMPORTANT]
> It is highly recommended that access control is done solely through these
> resource specs if `deployACLs` is enabled. This will prevent unintentional
> addition or removal of access.

### Deploying with the Data Foundation

By default, `config.json` > `deployDataMesh` enables deploying the Data Mesh
asset descriptions at the end of each workload build step. This default
configuration doesn't require enabling any additional APIs or roles. Additional
features of the Data Mesh can be deployed with the data foundation by enabling
the deployment options above, the required APIs and roles, and modifying the
associated resource specs.

### Deploying Alone

To deploy the Data mesh alone, users can use
[`common/data_mesh/deploy_data_mesh.py`](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/src/common/data_mesh/deploy_data_mesh.py).
This utility is used during the build processes to deploy the data mesh one
workload at a time, but when called directly it may also be used to deploy
multiple workloads at once. The workloads for the specs to be deployed should be
enabled in `config.json`. For example, ensure that `deploySAP` is `true` if
deploying the Data Mesh for SAP,

To ensure that you are deploying with required packages and versions, you can
run the utility from the same image used by the Cortex deployment process:

```
# Run container interactively
docker container run -it gcr.io/kittycorn-public/deploy-kittycorn:v2.0

# Clone the repo
git clone --recurse-submodules https://github.com/GoogleCloudPlatform/cortex-data-foundation

# Navigate into the repo
cd cortex-data-foundation
```

For help with the available parameters and their usage, run:

```
python src/common/data_mesh/deploy_data_mesh.py -h
```

Example invocation for SAP ECC:

```
python src/common/data_mesh/deploy_data_mesh.py \
  --config-file config/config.json \
  --lake-directories \
      src/SAP/SAP_REPORTING/config/ecc/lakes \
  --tag-template-directories \
      src/SAP/SAP_REPORTING/config/ecc/tag_templates \
  --policy-directories \
      src/SAP/SAP_REPORTING/config/ecc/policy_taxonomies \
  --annotation-directories \
      src/SAP/SAP_REPORTING/config/ecc/annotations
```

See the [Spec Directories](#spec-directories) section for info on directory
locations.

#### Overwrite {#overwrite}

> [!CAUTION]
> Read this section thoroughly before using the overwrite option.

By default, deploying Data Mesh will not overwrite any existing resources or
annotations. However, the `--overwrite` flag can be enabled when deploying the
Data Mesh alone to change the deployment in the following ways.

Overwriting metadata resources like Lakes, Catalog Tag Templates, and Policy
Tags will delete any existing resources that share the same names, however it
will not touch existing resources with different names. Note that this means if
a resource spec is removed entirely from the YAML file and then the Data Mesh
is redeployed with overwrite enabled, that resource spec will not be deleted
because there will be no name collision. This is so the Cortex Data Mesh
deployment doesn't impact existing resources that may be in use.

For nested resources like Lakes and Zones, overwriting a a resource will remove
all of its children. For example overwriting a Lake will also remove its
existing zones and asset references. For Catalog Tag Templates and Policy Tags
that are overwritten, the existing associated annotation references will be
removed from the assets as well. Overwriting Catalog Tags on an asset annotation
will only overwrite existing instances of Catalog Tags that share the same
template.

Asset and field description overwrites will only take effect if there is a valid
non-empty new description provided that conflicts with the existing description.

On the other hand, ACLs behave differently and overwriting ACLs will remove all
existing principals (with the exception of asset level owners). This is because
the principals being omitted from access policies are equally important to
principals being granted access.

## Exploring the Data Mesh

After deploying the Data Mesh, users can
[Search and view the data assets with Data Catalog](https://cloud.google.com/data-catalog/docs/how-to/search).
This includes the ability to discover assets based on Catalog Tag values that
were applied. Users can also begin manually creating and applying Catalog
Glossary terms if desired.

Access policies that were deployed can be viewed on the BigQuery Schema page to
see the policies applied on a particular asset at each level.

## Data Lineage

Users may find it useful to
[enable](https://cloud.google.com/data-catalog/docs/how-to/lineage-gcp) and
visualize the lineage between BigQuery assets. Lineage can also be accessed
programmatically through the
[API](https://cloud.google.com/python/docs/reference/lineage/latest). Data
Lineage currently only supports asset level lineage. Data Lineage is not currently
intertwined with the Cortex Data Mesh, however new features may be introduced
in the future that utilize Lineage.

For any Cortex Data Mesh feature requests, reach out to
[cortex-support@google.com](mailto:cortex-support@google.com)
