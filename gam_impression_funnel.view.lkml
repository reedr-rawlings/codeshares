view: gam_impression_funnel {
  derived_table: {
    sql_trigger_value: SELECT CURRENT_DATE() ;;
    sql: select COALESCE(user_impression_metrics.userid, user_click_metrics.userid) as userid
                , COALESCE(user_impression_metrics.orderid, user_click_metrics.orderid) as orderid
                , COALESCE(user_impression_metrics.lineitemid, user_click_metrics.lineitemid) as lineitemid
                , COALESCE(user_impression_metrics.CreativeID, user_click_metrics.CreativeID) as CreativeID
                , COALESCE(user_impression_metrics._data_date, user_click_metrics._data_date) as _data_date
                , COALESCE(user_impression_metrics.AdvertiserId, user_click_metrics.AdvertiserId) as AdvertiserId
                , COALESCE(user_impression_metrics.CustomTargeting, user_click_metrics.CustomTargeting) as CustomTargeting
                , COALESCE(user_impression_metrics.udid, user_click_metrics.udid) as udid
                , COALESCE(user_impression_metrics.zip_code, user_click_metrics.zip_code) as zip_code
                , COALESCE(user_impression_metrics.state_region, user_click_metrics.state_region) as state_region
                , COALESCE(user_impression_metrics.country_code, user_click_metrics.country_code) as country_code


                , first_impression
                , latest_impression
                , count_impressions

                  , first_click
                  , latest_click
                  , count_clicks
            from
            (select userid
                , orderid
                , lineitemid
                , CreativeID
                , _data_date
                , AdvertiserId
                , CustomTargeting
                , REGEXP_EXTRACT(CustomTargeting, 'udid=(.*).;') as udid
                -- , adunitid
                , min(postalcode) as zip_code
                , min(region) as state_region
                , min(country) as country_code
                , min(time) as first_impression
                , max(time) as latest_impression
                , count(*) as count_impressions
            from `ekoblov-test.dfp.impression_8264` as gam_impressions
            -- where userid <> '' and userid is not null
            WHERE
            -- if dev --  (((gam_impressions._DATA_DATE ) >= ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -7 DAY) AS TIMESTAMP), DAY)))) AND (gam_impressions._DATA_DATE ) < ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -7 DAY), INTERVAL 8 DAY) AS TIMESTAMP), DAY))))))
            -- if prod -- (((gam_impressions._DATA_DATE ) >= ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -30 DAY) AS TIMESTAMP), DAY)))) AND (gam_impressions._DATA_DATE ) < ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -30 DAY), INTERVAL 31 DAY) AS TIMESTAMP), DAY))))))

            group by 1,2,3,4,5,6,7,8 ) as user_impression_metrics

            full outer join

            (select
                userid
                , orderid
                , lineitemid
                , CreativeID
                , _data_date
                , AdvertiserId
                , CustomTargeting
                , REGEXP_EXTRACT(CustomTargeting, 'udid=(.*).;') as udid
                , min(postalcode) as zip_code
                , min(region) as state_region
                , min(country) as country_code

                , min(time) as first_click
                , max(time) as latest_click
                , count(*) as count_clicks
            from `ekoblov-test.dfp.impression_8264` as gam_clicks  --may need to swap table name with `NetworkClicks` depending on your naming convention
            -- where userid <> '' and userid is not null
            WHERE
            -- if dev --  (((gam_clicks._DATA_DATE ) >= ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -7 DAY) AS TIMESTAMP), DAY)))) AND (gam_clicks._DATA_DATE ) < ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -7 DAY), INTERVAL 8 DAY) AS TIMESTAMP), DAY))))))
            -- if prod -- (((gam_clicks._DATA_DATE ) >= ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -30 DAY) AS TIMESTAMP), DAY)))) AND (gam_clicks._DATA_DATE ) < ((DATE(TIMESTAMP_TRUNC(CAST(TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY), INTERVAL -30 DAY), INTERVAL 31 DAY) AS TIMESTAMP), DAY))))))
            group by 1,2,3,4,5,6,7,8
            ) as user_click_metrics

            on
            user_impression_metrics.userid = user_click_metrics.userid
            and user_impression_metrics.orderid = user_click_metrics.orderid
            -- and user_impression_metrics.adunitid = user_click_metrics.adunitid
            and user_impression_metrics.lineitemid = user_click_metrics.lineitemid
            and user_impression_metrics.CreativeID = user_click_metrics.CreativeID
            and user_impression_metrics._data_date = user_click_metrics._data_date
            and user_impression_metrics.AdvertiserId = user_click_metrics.AdvertiserId
            and user_impression_metrics.CustomTargeting = user_click_metrics.CustomTargeting ;;
      partition_keys: ["_data_date"]

  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.userid ;;
  }

  dimension_group: event {
    type: time
    timeframes: [raw,date,month,year,day_of_week,week_of_year,month_num]
    sql: TIMESTAMP(${TABLE}._data_date) ;;
  }

  dimension: custom_targeting {
    type: string
    sql: ${TABLE}.CustomTargeting ;;
    hidden: yes
  }

  dimension: udid {
    label: "UDID"
    description: "udid=(.*).;"
    type: string
    # sql: REGEXP_EXTRACT(${custom_targeting}, 'udid=(.*).;') ;;
    sql: ${TABLE}.udid ;;
  }

  dimension: ad_bundle {
    description: "when custom targeting = briefing then briefing else customer"
    type: string
    sql: case when ${custom_targeting} like '%Briefing%' then 'Briefing'
        else 'Customer' end ;;
  }

  dimension: is_direct {
    label: "Is Direct?"
    description: "where advertiser id is not null"
    type: yesno
    sql: ${advertiser_id} is not null ;;
  }

  dimension: is_lat {
    label: "Is LAT_FLAG?"
    description: "where user id is not null"
    type: yesno
    sql: ${user_id} is not null ;;
  }

  dimension: order_id {
    type: number
    sql: ${TABLE}.orderid ;;
  }

  dimension: advertiser_id {
    type: number
    sql: ${TABLE}.AdvertiserId ;;
  }

  dimension: ad_unit_id {
    type: number
    sql: ${TABLE}.adunitid ;;
    hidden: yes
  }

  dimension: line_item_id {
    type: number
    sql: ${TABLE}.lineitemid ;;
  }

  dimension: creative_id {
    type: number
    sql: ${TABLE}.CreativeID ;;
  }

  dimension: zip_code {
    type: zipcode
    sql: ${TABLE}.zip_code ;;
    map_layer_name: us_zipcode_tabulation_areas
  }

  dimension: state {
    description: "state or region depending on country"
    type: string
    sql: ${TABLE}.state_region ;;
    map_layer_name: us_states
  }

  dimension: country_code {
    label: "Country"
    type: string
    sql: ${TABLE}.country_code ;;
    map_layer_name: countries
  }

  dimension: first_impression {
    type: string
    sql: ${TABLE}.first_impression ;;
    hidden: yes
  }

  dimension: latest_impression {
    type: string
    sql: ${TABLE}.latest_impression ;;
    hidden: yes
  }

  dimension: count_impressions {
    type: number
    hidden: yes
    sql: ${TABLE}.count_impressions ;;
  }

  dimension: first_click {
    type: string
    sql: ${TABLE}.first_click ;;
    hidden: yes
  }

  dimension: latest_click {
    type: string
    sql: ${TABLE}.latest_click ;;
    hidden: yes
  }

  dimension: count_clicks {
    hidden: yes
    type: number
    sql: ${TABLE}.count_clicks ;;
  }

  measure: user_count {
    description: "count of distinct udid"
    type: count_distinct
    sql: ${udid} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
  }

  measure: total_impressions {
    description: "total impression count"
    type: sum
    sql: ${count_impressions} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
  }

  measure: total_clicks {
    description: "total click count from impressions"
    type: sum
    sql: ${count_clicks} ;;
    value_format_name: decimal_0
    drill_fields: [detail*]
  }

  measure: click_through_rate {
    description: "CTR"
    type: number
    sql: ${total_clicks}*1.0/(NULLIF(${total_impressions},0)) ;;
    value_format_name: percent_4
    drill_fields: [detail*]
  }




  ## previous period analysis

  filter: previous_period_filter {
    type: date
    description: "Use this filter for period analysis"
    sql: ${previous_period} is not null ;;
  }

  dimension: previous_period {
    type: string
    description: "The reporting period as selected by the Previous Period Filter"
    sql:
      CASE
        WHEN {% date_start previous_period_filter %} is not null AND {% date_end previous_period_filter %} is not null /* date ranges or in the past x days */
          THEN
            CASE
              WHEN ${event_raw} >=  {% date_start previous_period_filter %}
                AND ${event_raw} <= {% date_end previous_period_filter %}
                THEN 'This Period'
              WHEN ${event_raw} >=
              TIMESTAMP_ADD(TIMESTAMP_ADD({% date_start previous_period_filter %}, INTERVAL -1 DAY ), INTERVAL
                -1*DATE_DIFF(DATE({% date_end previous_period_filter %}), DATE({% date_start previous_period_filter %}), DAY) + 1 DAY)
                AND ${event_raw} <=
                TIMESTAMP_ADD({% date_start previous_period_filter %}, INTERVAL -1 DAY )
                THEN 'Previous Period'
            END
          END ;;
  }

  set: detail {
    fields: [
      user_id,
      order_id,
      ad_unit_id,
      line_item_id,
      zip_code,
      state,
      country_code,
      first_impression,
      latest_impression,
      count_impressions,
      first_click,
      latest_click,
      count_clicks
    ]
  }
}
