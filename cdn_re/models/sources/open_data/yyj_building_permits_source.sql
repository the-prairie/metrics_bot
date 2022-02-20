with 
  source as (
      select *
      from {{ source('open_data', 'yyj_building_permits') }}
  ),
  
  
  renamed as (
      
      select  
        attributes_objectid as id,
        attributes_gislink as gis_link,
        attributes_category as category,
        attributes_type as type,
        attributes_permittype as permit_type,
        initcap(
            regexp_extract(attributes_type, r"-([\s\S]*)$")
         ) as permit_subtype,
        attributes_permitno as permit_no,
        attributes_subject as subject,
        initcap(attributes_status) as status,
        attributes_purpose as purpose,
        parse_date("%Y%m%d", attributes_issueddate) as issued_on,
        attributes_unit as unit,
        attributes_house as house_number,
        attributes_street as street_name,
        attributes_address as address,
        attributes_completed_date as completed_on,
        attributes_created_date as created_on,
        attributes_parceltype as parcel_type,
        attributes_auc as auc,
        attributes_actualuse as actual_use,
        attributes_neighbourhood as neighbourhood,
        st_geogpoint(attributes_x_long, attributes_y_lat) as geopoint,
        attributes_x_long as longitude,
        attributes_y_lat as latitude,
        initcap(attributes_contacttype) as contact_type,
        initcap(attributes_name) as contact_name,
        attributes_mailing_address as contact_mailing_address,
        attributes_phone as contact_phone,
        attributes_cell as contact_cell,
        attributes_email as contact_email,
        attributes_fax as contact_fax,
        attributes_auc_group as auc_group,
        attributes_work_value as work_value,
        _extracted_at
      
      from source
  ),

  calculate_approval_time as (

      select *,
        date_diff(issued_on, created_on, day) as days_to_approval
      from renamed
  )

  select *
  from calculate_approval_time