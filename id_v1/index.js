"use strict";

const AthenaExpress = require("athena-express"),
    aws = require("aws-sdk");

/* 
 * AWS Credentials are not required here
 * because the IAM Role assumed by this Lambda
 * has the necessary permission to execute Athena queries
 * and store the result in Amazon S3 bucket
 */

const athenaExpressConfig = {
    aws,
    db: "geocore_metadata",
    getStats: true
};

const athenaExpress = new AthenaExpress(athenaExpressConfig);

exports.handler = async(event, context, callback) => {
    
    var uuid = event.id;
    var lang = event.lang;

    var id = "COALESCE(features_properties_id, 'N/A') AS id";
    var coordinates = "features_geometry_coordinates AS coordinates";
    var publication_date = "COALESCE(features_properties_date_published_date, 'N/A') AS published";
    var options = "TRY(CAST(features_properties_options AS JSON)) AS options";
    var contact = "TRY(CAST(features_properties_contact AS JSON)) AS contact";
    var topicCategory = "COALESCE(features_properties_topicCategory, 'N/A') AS topicCategory";
    var created_date = "COALESCE(features_properties_date_created_date, 'N/A') AS created";
    var spatialRepresentation = "COALESCE(features_properties_spatialRepresentation, 'N/A') AS spatialRepresentation";
    var type = "COALESCE(features_properties_type, 'N/A') AS type";
    var temporalExtent = "MAP_FROM_ENTRIES(ARRAY[('begin', features_properties_temporalExtent_begin), ('end', features_properties_temporalExtent_end)]) AS temporalExtent";
    var graphicOverview = "features_properties_graphicOverview AS graphicOverview";
    var language = "COALESCE(features_properties_language, 'N/A') AS language";
    var refSys = "COALESCE(features_properties_refSys, 'N/A') AS refSys";
    var refSys_version = "COALESCE(features_properties_refSys_version, 'N/A') AS refSys_version";
    var status = "COALESCE(features_properties_status, 'N/A') AS status";
    var maintenance = "COALESCE(features_properties_maintenance, 'N/A') AS maintenance";
    var metadataStandard = "COALESCE(features_properties_metadataStandard_en, 'N/A') AS metadataStandard";
    var metadataStandardVersion = "COALESCE(features_properties_metadataStandardVersion, 'N/A') AS metadataStandardVersion";
    var distributionFormat_name = "COALESCE(features_properties_distributionFormat_name, 'N/A') AS distributionFormat_name";
    var distributionFormat_format = "COALESCE(features_properties_distributionFormat_format, 'N/A') AS distributionFormat_format";
    var accessConstraints = "COALESCE(features_properties_accessConstraints, 'N/A') AS accessConstraints";
    var otherConstraints = "COALESCE(features_properties_otherConstraints_en, 'N/A') AS otherConstraints";
    var dateStamp = "COALESCE(features_properties_dateStamp, 'N/A') AS dateStamp";
    var dataSetURI = "COALESCE(features_properties_dataSetURI, 'N/A') AS dataSetURI";
    var locale = "MAP_FROM_ENTRIES(ARRAY[('language', features_properties_locale_language), ('country', features_properties_locale_country), ('encoding', features_properties_locale_encoding)]) AS locale";
    var characterSet = "COALESCE(features_properties_characterSet, 'N/A') AS characterSet";
    var environmentDescription = "COALESCE(features_properties_environmentDescription, 'N/A') as environmentDescription";
    var supplementalInformation = "COALESCE(features_properties_supplementalInformation_en, 'N/A') AS supplementalInformation";
    var credits = "TRY(CAST(features_properties_credits AS JSON)) AS credits";
    var cited = "TRY(CAST(features_properties_cited AS JSON)) AS cited";
    var distributor = "TRY(CAST(features_properties_distributor AS JSON)) AS distributor";
    var title_en = "COALESCE(features_properties_title_en, 'N/A') AS title_en";
    var title_fr = "COALESCE(features_properties_title_fr, 'N/A') AS title_fr";
    var plugins = "TRY(CAST(features_properties_plugins AS JSON)) AS plugins";
    var source_system_name = "COALESCE(features_properties_sourcesystemname, 'N/A') as source_system_name";
    /* var rangeSlider_enable = "COALESCE(features_properties_plugins_rangeSlider_enable, 'N/A') AS rangeSlider_enable";
    var rangeSlider_open = "COALESCE(features_properties_plugins_rangeSlider_enable, 'N/A') AS rangeSlider_open";
    var rangeSlider_contols = "COALESCE(features_properties_plugins_rangeSlider_controls, 'N/A') AS rangeSlider_controls";
    var rangeSlider_params_type = "COALESCE(features_properties_plugins_rangeSlider_params_type, 'N/A') AS rangeSlider_params_type";
    var rangeSlider_params_delay = "COALESCE(features_properties_plugins_rangeSlider_params_delay, 'N/A') AS rangeSlider_params_delay";
    var rangeSlider_params_rangeType = "COALESCE(features_properties_plugins_rangeSlider_params_rangeType, 'N/A') AS rangeSlider_params_rangeType";
    var rangeSlider_params_stepType = "COALESCE(features_properties_plugins_rangeSlider_params_stepType, 'N/A') AS rangeSlider_params_stepType";
    var rangeSlider_params_precision = "COALESCE(features_properties_plugins_rangeSlider_params_precision, 'N/A') AS rangeSlider_params_precision";
    var rangeSlider_params_rangeInterval = "COALESCE(features_properties_plugins_rangeSlider_params_rangeInterval, 'N/A') AS rangeSlider_params_rangeInterval";
    var rangeSlider_params_startRangeEnd = "COALESCE(features_properties_plugins_rangeSlider_params_startRangeEnd, 'N/A') AS rangeSlider_params_startRangeEnd";
    var rangeSlider_params_range_min = "COALESCE(features_properties_plugins_rangeSlider_params_range_min, 'N/A') AS rangeSlider_params_range_min";
    var rangeSlider_params_range_max = "COALESCE(features_properties_plugins_rangeSlider_params_range_max, 'N/A') AS rangeSlider_params_range_max";
    var rangeSlider_params_limit_min = "COALESCE(features_properties_plugins_rangeSlider_params_limit_min, 'N/A') AS rangeSlider_params_limit_min";
    var rangeSlider_params_limit_max = "COALESCE(features_properties_plugins_rangeSlider_params_limit_max, 'N/A') AS rangeSlider_params_limit_max";
    var rangeSlider_params_limit_staticItems = "COALESCE(features_properties_plugins_rangeSlider_params_limit_staticItems, 'N/A') AS rangeSlider_params_limit_staticItems";
    var rangeSlider_params_units = "COALESCE(features_properties_plugins_rangeSlider_params_units, 'N/A') AS rangeSlider_params_units";
    var rangeSlider_maximize = "COALESCE(features_properties_plugins_rangeSlider_maximize , 'N/A') AS rangeSlider_maximize";
    var rangeSlider_maximizeDesc = "COALESCE(features_properties_plugins_rangeSlider_maximizeDesc , 'N/A') AS rangeSlider_maximizeDesc";
    var rangeSlider_autorun = "COALESCE(features_properties_plugins_rangeSlider_autorun , 'N/A') AS rangeSlider_autorun";
    var rangeSlider_loop = "COALESCE(features_properties_plugins_rangeSlider_loop , 'N/A') AS rangeSlider_loop";
    var rangeSlider_reverse = "COALESCE(features_properties_plugins_rangeSlider_reverse , 'N/A') AS rangeSlider_reverse";
    var rangeSlider_lock = "COALESCE(features_properties_plugins_rangeSlider_lock , 'N/A') AS rangeSlider_lock";
    var coordInfo_enable = "COALESCE(features_properties_plugins_coordInfo_enable , 'N/A') AS coordInfo_enable";
    var areasOfInterest_enable = "COALESCE(features_properties_plugins_areasOfInterest_enable , 'N/A') AS areasOfInterest_enable";
    var chart_enable = "COALESCE(features_properties_plugins_chart_enable , 'N/A') AS chart_enable";
    var chart_type = "COALESCE(features_properties_plugins_chart_type , 'N/A') AS chart_type";
    var chart_options_colors = "COALESCE(features_properties_plugins_chart_options_colors , 'N/A') AS chart_options_colors";
    var chart_options_cutOut = "COALESCE(features_properties_plugins_chart_options_cutOut , 'N/A') AS chart_options_cutOut";
    var chart_axis_xAxis_type = "COALESCE(features_properties_plugins_chart_axis_xAxis_type , 'N/A') AS chart_axis_xAxis_type";
    var chart_axis_xAxis_values = "COALESCE(features_properties_plugins_chart_axis_xAxis_values , 'N/A') AS chart_axis_xAxis_values";
    var chart_axis_xAxis_split = "COALESCE(features_properties_plugins_chart_axis_xAxis_split , 'N/A') AS chart_axis_xAxis_split";
    var chart_axis_yAxis_type = "COALESCE(features_properties_plugins_chart_axis_yAxis_type , 'N/A') AS chart_axis_yAxis_type";
    var chart_axis_yAxis_values = "COALESCE(features_properties_plugins_chart_axis_yAxis_values , 'N/A') AS chart_axis_yAxis_values";
    var chart_axis_yAxis_split = "COALESCE(features_properties_plugins_chart_axis_yAxis_split , 'N/A') AS chart_axis_yAxis_split";
    var chart_layers = "COALESCE(features_properties_plugins_chart_layers , 'N/A') AS chart_layers";
    var chart_labelsPie_type = "COALESCE(features_properties_plugins_chart_labelsPie_type , 'N/A') AS chart_labelsPie_type";
    var chart_labelsPie_values = "COALESCE(features_properties_plugins_chart_labelsPie_values , 'N/A') AS chart_labelsPie_values";
    var chart_labelsPie_split = "COALESCE(features_properties_plugins_chart_labelsPie_split , 'N/A') AS chart_labelsPie_split";
    var swiper_enable = "COALESCE(features_properties_plugins_swiper_enable , 'N/A') AS swiper_enable";
    var swiper_type = "COALESCE(features_properties_plugins_swiper_type , 'N/A') AS swiper_type";
    var swiper_keyboardOffset = "COALESCE(features_properties_plugins_swiper_keyboardOffset , 'N/A') AS swiper_keyboardOffset";
    var draw_enable = "COALESCE(features_properties_plugins_draw_enable , 'N/A') AS draw_enable";
    var draw_open = "COALESCE(features_properties_plugins_draw_open , 'N/A') AS draw_open";
    var draw_tools = "COALESCE(features_properties_plugins_draw_tools , 'N/A') AS draw_tools";
    var thematicSlider_enable = "COALESCE(features_properties_plugins_thematicSlider_enable , 'N/A') AS thematicSlider_enable";
    var thematicSlider_open = "COALESCE(features_properties_plugins_thematicSlider_open , 'N/A') AS thematicSlider_open";
    var thematicSlider_autorun = "COALESCE(features_properties_plugins_thematicSlider_autorun , 'N/A') AS thematicSlider_autorun";
    var thematicSlider_loop = "COALESCE(features_properties_plugins_thematicSlider_loop , 'N/A') AS thematicSlider_loop";
    var thematicSlider_description = "COALESCE(features_properties_plugins_thematicSlider_description , 'N/A') AS thematicSlider_description";
    var thematicSlider_slider = "COALESCE(features_properties_plugins_thematicSlider_slider , 'N/A') AS thematicSlider_slider";
    var thematicSlider_stack = "COALESCE(features_properties_plugins_thematicSlider_stack , 'N/A') AS thematicSlider_stack";
    var thematicSlider_legendStack = "COALESCE(features_properties_plugins_thematicSlider_legendStack , 'N/A') AS thematicSlider_legendStack"; */
    

    let keywords;
    let description;
    let useLimits;
    /* let chart_title;
    let chart_axis_xAxis_title;
    let chart_axis_yAxis_title; */
    
    if (lang === "fr") {
        
        keywords = "COALESCE(features_properties_keywords_fr, 'N/A') AS keywords";
        description = "COALESCE(features_properties_description_fr, 'N/A') AS description";
        useLimits = "COALESCE(features_properties_useLimits_fr, 'N/A') AS useLimits";
        /* chart_title = "COALESCE(features_properties_plugins_chart_title_fr , 'N/A') AS chart_title";
        chart_axis_xAxis_title = "COALESCE(features_properties_plugins_chart_axis_xAxis_title_fr , 'N/A') AS chart_axis_xAxis_title";
        chart_axis_yAxis_title = "COALESCE(features_properties_plugins_chart_axis_yAxis_title_fr , 'N/A') AS chart_axis_yAxis_title"; */
    
        
    } else {
        
        keywords = "COALESCE(features_properties_keywords_en, 'N/A') AS keywords";
        description = "COALESCE(features_properties_description_en, 'N/A') AS description";
        useLimits = "COALESCE(features_properties_useLimits_en, 'N/A') AS useLimits";
        /* chart_title = "COALESCE(features_properties_plugins_chart_title_en , 'N/A') AS chart_title";
        chart_axis_xAxis_title = "COALESCE(features_properties_plugins_chart_axis_xAxis_title_en , 'N/A') AS chart_axis_xAxis_title";
        chart_axis_yAxis_title = "COALESCE(features_properties_plugins_chart_axis_yAxis_title_en , 'N/A') AS chart_axis_yAxis_title"; */
        
    }

    //var display_fields = "" + id + ", " + coordinates + ", " + title + ", " + description + ", " + publication_date + ", " + keywords + ", " + options + ", " + contact + ", " + topicCategory + ", " + created_date + ", " + spatialRepresentation + ", " + type + ", " + temporalExtent + ", " + graphicOverview + ", " + language + ", " + refSys + "";
    //, " + supplementalInformation + ", " + credits + ", " + distributor + "";

    //var display_fields = "" + id + ", " + coordinates + ", " + title_en + ", " + title_fr + ", " + description + ", " + publication_date + ", " + keywords + ", " + options + ", " + contact + ", " + topicCategory + ", " + created_date + ", " + spatialRepresentation + ", " + type + ", " + temporalExtent + ", " + refSys + ", " + refSys_version + ", " + status + ", " + maintenance + ", " + metadataStandard + ", " + metadataStandardVersion + ", " + graphicOverview + ", " + distributionFormat_name + ", " + distributionFormat_format + ", " + useLimits + ", " + accessConstraints + ", " + otherConstraints + ", " + dateStamp + ", " + dataSetURI + ", " + locale + ", " + language + ", " + characterSet + ", " + environmentDescription + ", " + supplementalInformation + ", " + credits + ", " + distributor + ", " + rangeSlider_enable + ", " + rangeSlider_open + ", " + rangeSlider_contols + ", " + rangeSlider_params_type + ", " + rangeSlider_params_delay + ", " + rangeSlider_params_rangeType + ", " + rangeSlider_params_stepType + ", " + rangeSlider_params_precision + ", " + rangeSlider_params_rangeInterval + ", " + rangeSlider_params_startRangeEnd + ", " + rangeSlider_params_range_min + ", " + rangeSlider_params_range_max + ", " + rangeSlider_params_limit_min + ", " + rangeSlider_params_limit_max + ", " + rangeSlider_params_limit_staticItems + ", " + rangeSlider_params_units + ", " + rangeSlider_maximize + ", " + rangeSlider_maximizeDesc + ", " + rangeSlider_autorun + ", " + rangeSlider_loop + ", " + rangeSlider_reverse + ", " + rangeSlider_lock + ", " + coordInfo_enable + ", " + areasOfInterest_enable + ", " + chart_enable + ", " + chart_type + ", " + chart_options_colors + ", " + chart_options_cutOut +  ", " + chart_title + ", " + chart_axis_xAxis_title + ", " + chart_axis_yAxis_title + ", " + chart_axis_xAxis_type + ", " + chart_axis_xAxis_values + ", " + chart_axis_xAxis_split + ", " + chart_axis_yAxis_type + ", " + chart_axis_xAxis_split + ", " + chart_axis_yAxis_values + ", " + chart_axis_yAxis_split + ", " + chart_layers + ", " + chart_labelsPie_type + ", " + chart_labelsPie_values + ", " + chart_labelsPie_split + ", " + swiper_enable + ", " + swiper_type + ", " + swiper_keyboardOffset + ", " + draw_enable + ", " + draw_open + ", " + draw_tools + ", " + thematicSlider_enable + ", " + thematicSlider_open + ", " + thematicSlider_autorun + ", " + thematicSlider_loop + ", " + thematicSlider_description + ", " + thematicSlider_slider + ", " + thematicSlider_stack + ", " + thematicSlider_legendStack + "";
    var display_fields = "" + id + ", " + coordinates + ", " + title_en + ", " + title_fr + ", " + description + ", " + publication_date + ", " + keywords + ", " + options + ", " + contact + ", " + topicCategory + ", " + created_date + ", " + spatialRepresentation + ", " + type + ", " + temporalExtent + ", " + refSys + ", " + refSys_version + ", " + status + ", " + maintenance + ", " + metadataStandard + ", " + metadataStandardVersion + ", " + graphicOverview + ", " + distributionFormat_name + ", " + distributionFormat_format + ", " + useLimits + ", " + accessConstraints + ", " + otherConstraints + ", " + dateStamp + ", " + dataSetURI + ", " + locale + ", " + language + ", " + characterSet + ", " + environmentDescription + ", " + supplementalInformation + ", " + credits + ", " + distributor + ", " + source_system_name + ", " + plugins + "";
    
    let sqlQuery;
    
    
    sqlQuery = "SELECT " + display_fields + " FROM metadata WHERE features_properties_id = '" + uuid + "'";
    

    try {
        let results = await athenaExpress.query(sqlQuery);

        var result = results.Items[0];
                
        var plugins_string = result.plugins.replace(/\\\"/g,'"').replace(/\"/g,'"').replace(/""/g,'"').slice(1,-1);
        if (plugins_string != "NaN")
        {
            plugins_string = JSON.parse(plugins_string)
        }

        var response =     {
            "Items": [
                {
                "id": result.id,
                "coordinates": result.coordinates,
                "title_en": result.title_en,
                "title_fr": result.title_fr,
                "description": result.description,
                "published": result.published,
                "keywords": result.keywords,
                "options": result.options,
                "contact": result.contact,
                "topicCategory": result.topicCategory,
                "created": result.created,
                "spatialRepresentation": result.spatialRepresentation,
                "type": result.type,
                "temporalExtent": result.temporalExtent,
                "refSys": result.refSys,
                "refSys_version": result.refSys_version,
                "status": result.status,
                "maintenance": result.maintenance,
                "metadataStandard": result.metadataStandard,
                "metadataStandardVersion": result.metadataStandardVersion,
                "graphicOverview": result.graphicOverview,
                "distributionFormat_name": result.distributionFormat_name,
                "distributionFormat_format": result.distributionFormat_format,
                "useLimits": result.useLimits,
                "accessConstraints": result.accessConstraints,
                "otherConstraints": result.otherConstraints,
                "dateStamp": result.dateStamp,
                "dataSetURI": result.dataSetURI,
                "locale": result.locale,
                "language": result.language,
                "characterSet": result.characterSet,
                "environmentDescription": result.environmentDescription,
                "supplementalInformation": result.supplementalInformation,
                "credits": result.credits,
                "distributor": result.distributor,
                "plugins": plugins_string,
                "source_system_name": result.source_system_name
        }]};
        
        // console.log(response);
        //console.log(results);
        //context.succeed(response);
        callback(null, response);
    }

    catch (error) {
        callback(error, null);
    }
};