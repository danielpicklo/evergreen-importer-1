// index.js
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { Storage }               = require('@google-cloud/storage');
const axios                     = require('axios');
const FormData                  = require('form-data');
const minimist                  = require('minimist');

const firestore = new Firestore({
  projectId: 'evergreen-45696013',
  databaseId: 'imports'
});
const storage   = new Storage();

const BUCKET_NAME     = 'evergreen-import-storage';
const RUNS_COLLECTION = 'imports';
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HUBSPOT_UPLOAD  = 'https://api.hubapi.com/crm/v3/imports/';

// Define your batches by _base_ filename (no date or part suffix)
const BATCH_FILES = {
  1: ['TMZip'],
  2: ['SalesRep'],
  3: ['CM'],
  4: ['PRODUCTS_EVERGREEN'],
  5: ['Evergreen_OH_Full'],
  6: ['Evergreen_OD_Delta']
};

// Column mappings keyed by base filename
const FILE_SCHEMA = {
  "Evergreen_OD_Delta": [
    {'columnObjectTypeId': '0-8', 'columnName': 'Key_Number', 'propertyName': 'update'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Number', 'propertyName': 'order_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'PO_Number', 'propertyName': 'po_number'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Item_ID', 'propertyName': 'hs_sku'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Item_Description', 'propertyName': 'name'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Item_Price', 'propertyName': 'price'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Total_Price', 'propertyName': 'total_price'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Quantity_Ordered', 'propertyName': 'quantity'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Quantity_Shipped', 'propertyName': 'quantity_shipped'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Quantity_Backordered', 'propertyName': 'quantity_backordered'},
    {'columnObjectTypeId': '0-8', 'columnName': 'UPC', 'propertyName': 'upc'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Claim_Quantity', 'propertyName': 'claim_quantity'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Checkin_Quantity', 'propertyName': 'checkin_quantity'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Return_Reason', 'propertyName': 'return_reason'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Return_Status', 'propertyName': 'return_status'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Desired_Resolution', 'propertyName': 'desired_resolution'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Claim_Identifier', 'propertyName': 'claim_identifier'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Claim_Number', 'propertyName': 'claim_number'},
    {'columnName': 'Inv_Available_Ship_Date', ignored: true},
    {'columnObjectTypeId': '0-8', 'columnName': 'Brand', 'propertyName': 'brand'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Currency', 'propertyName': 'hs_line_item_currency_code'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Supplier', 'propertyName': 'supplier'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Length_in', 'propertyName': 'length'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Width_in', 'propertyName': 'width'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Height_in', 'propertyName': 'height'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Weight_lbs', 'propertyName': 'weight'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Wholesale_Price', 'propertyName': 'wholesale_price'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Short_Description', 'propertyName': 'long_description'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Category', 'propertyName': 'categories'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Collection', 'propertyName': 'collection'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Country_of_Origin', 'propertyName': 'country_of_origin'},
    {'columnObjectTypeId': '0-8', 'columnName': 'Box_Number', 'propertyName': 'box_number'},
    {'columnObjectTypeId': '0-8', 'columnName': 'PDFUrl', 'propertyName': 'pdf_url'},
    {'columnObjectTypeId': '0-8', 'columnName': 'IsCustomized', 'propertyName': 'is_customized'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Rep_ID', 'propertyName': 'rep_id'},
    {'columnObjectTypeId': '0-123', 'columnName': 'RepEmail', 'propertyName': 'rep_email'}],
  "Evergreen_OH_Full": [
    {'columnObjectTypeId': '0-2', 'columnName': 'Customer_Number', 'propertyName': 'account_number', 'columnType': 'HUBSPOT_ALTERNATE_ID'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Key_Number', 'propertyName': 'order_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Number', 'propertyName': 'hs_order_name'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Invoice_Number', 'propertyName': 'invoice_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Website_Order_Number', 'propertyName': 'website_order'},
    {'columnObjectTypeId': '0-123', 'columnName': 'PO_Number', 'propertyName': 'po_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Status', 'propertyName': 'hs_pipeline_stage'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Tracking_Number', 'propertyName': 'hs_shipping_tracking_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Source', 'propertyName': 'source'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Payment_Terms', 'propertyName': 'payment_terms'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Date', 'propertyName': 'hs_order_note'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Ship_Date', 'propertyName': 'ship_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Actual_Ship_Date', 'propertyName': 'actual_ship_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Requested_Ship_Date', 'propertyName': 'requested_ship_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Scheduled_Ship_Date', 'propertyName': 'scheduled_ship_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Invoice_Date', 'propertyName': 'invoice_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Cancel_Date', 'propertyName': 'cancel_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Notes', 'propertyName': 'order_notes'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Availability', 'propertyName': 'availability'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Rep_ID', 'propertyName': 'rep_id'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Rep_Org', 'propertyName': 'hs_pipeline'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Ship_Via', 'propertyName': 'ship_via'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Batch_Code', 'propertyName': 'batch_code'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Phone_Number', 'propertyName': 'hs_billing_address_phone'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Company_Name', 'propertyName': 'hs_billing_address_name'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_First_Name', 'propertyName': 'hs_billing_address_firstname'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Last_Name', 'propertyName': 'hs_billing_address_lastname'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Address_1', 'propertyName': 'hs_billing_address_street'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Address_2', 'propertyName': 'billing_street_2'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_City_APO_AFO', 'propertyName': 'hs_billing_address_city'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_State_Province_Region', 'propertyName': 'hs_billing_address_state'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Zip_Postal_Code', 'propertyName': 'hs_billing_address_postal_code'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Billing_Country', 'propertyName': 'hs_billing_address_country'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Phone_Number', 'propertyName': 'hs_shipping_address_phone'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Company_Name', 'propertyName': 'hs_shipping_address_name'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_First_Name', 'propertyName': 'shipping_first_name'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Last_Name', 'propertyName': 'shipping_last_name'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Address_1', 'propertyName': 'hs_shipping_address_street'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Address_2', 'propertyName': 'shipping_street_2'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_City_APO_AFO', 'propertyName': 'hs_shipping_address_city'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_State_Province_Region', 'propertyName': 'hs_shipping_address_state'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Zip_Postal_Code', 'propertyName': 'hs_shipping_address_postal_code'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipping_Country', 'propertyName': 'hs_shipping_address_country'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Subtotal', 'propertyName': 'subtotal'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Freight', 'propertyName': 'freight'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Promotion', 'propertyName': 'promotion'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Discount', 'propertyName': 'hs_order_discount'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Order_Total', 'propertyName': 'total_amount'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Payment', 'propertyName': 'payment'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Shipped_Total', 'propertyName': 'shipped_total'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Claim_Status', 'propertyName': 'claim_status'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Processed_Claim_Amount', 'propertyName': 'processed_claim_amount'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Claim_Adjustment', 'propertyName': 'claim_adjustment'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Credited_Amount', 'propertyName': 'credited_amount'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Open_Claim_Amount', 'propertyName': 'open_claim_amount'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Claim_Notes', 'propertyName': 'claim_notes'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Parent_Order_ID', 'propertyName': 'parent_order_id'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Internal_Status', 'propertyName': 'internal_status'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Balance_Due', 'propertyName': 'balance_due'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Spotlight_Order_Number', 'propertyName': 'spotlight_order_number'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Brand', 'propertyName': 'brand'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Invoice_Total', 'propertyName': 'invoice_total'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Finance_Charge', 'propertyName': 'finance_charge'},
    {'columnObjectTypeId': '0-123', 'columnName': 'Due_Date', 'propertyName': 'due_date'},
    {'columnObjectTypeId': '0-123', 'columnName': 'RepEmail', 'propertyName': 'rep_email'}],
  "PRODUCTS_EVERGREEN": [
    {'columnObjectTypeId': '0-7', 'columnName': 'SKU_Number', 'propertyName': 'hs_sku'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Group_SKU_Number', 'propertyName': 'group_sku_number'},
    {'columnName': 'Group_Name', ignored: true},
    {'columnName': 'Option_1', ignored: true},
    {'columnName': 'Option_1_Label', ignored: true},
    {'columnName': 'Is_Default_Variant', ignored: true},
    {'columnObjectTypeId': '0-7', 'columnName': 'Product_Name', 'propertyName': 'name'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Categories', 'propertyName': 'categories'},
    {'columnName': 'Video_ID', ignored: true},
    {'columnObjectTypeId': '0-7', 'columnName': 'Brand', 'propertyName': 'brand'},
    {'columnObjectTypeId': '0-7', 'columnName': 'US_Price', 'propertyName': 'price'},
    {'columnObjectTypeId': '0-7', 'columnName': 'US_Sale_Price', 'propertyName': 'sale_price'},
    {'columnObjectTypeId': '0-7', 'columnName': 'US_Case_Pack_Price', 'propertyName': 'case_pack_price'},
    {'columnObjectTypeId': '0-7', 'columnName': 'CAD_Case_Pack_Price', 'propertyName': 'case_pack_price_cad'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Case_Pack_Quantity', 'propertyName': 'case_pack_quantity'},
    {'columnObjectTypeId': '0-7', 'columnName': 'CAD_Price', 'propertyName': 'price_cad'},
    {'columnObjectTypeId': '0-7', 'columnName': 'CAD_Sale_Price', 'propertyName': 'sale_price_cad'},
    {'columnName': 'US_EOD_Price', ignored: true},
    {'columnName': 'CAD_EOD_Price', ignored: true},
    {'columnObjectTypeId': '0-7', 'columnName': 'Visible_To', 'propertyName': 'visible_to'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Minimum', 'propertyName': 'minimum'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Increment', 'propertyName': 'increment'},
    {'columnObjectTypeId': '0-7', 'columnName': 'UOM', 'propertyName': 'unit_of_measure'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Availability', 'propertyName': 'availability'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Flag_1_New', 'propertyName': 'flag_1_new'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Flag_2_Bestseller', 'propertyName': 'flag_2_bestseller'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Flag_3_Sale', 'propertyName': 'flag_3_sale'},
    {'columnName': 'Availability_Filter', ignored: true},
    {'columnObjectTypeId': '0-7', 'columnName': 'Sale_Tag_Filter', 'propertyName': 'sales_tag_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Team_Filter', 'propertyName': 'team_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'League_Filter', 'propertyName': 'league_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Color_Filter', 'propertyName': 'color_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Program_Filter', 'propertyName': 'program_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Product_Type_Filter', 'propertyName': 'product_type_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Features_Filter', 'propertyName': 'features_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Size_Filter', 'propertyName': 'size_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'LicensedCollections_Filter', 'propertyName': 'licensed_collections'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Merchandising_Filter', 'propertyName': 'merchandising_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Theme_Filter', 'propertyName': 'theme_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Holiday_Filter', 'propertyName': 'holiday_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Material_Filter', 'propertyName': 'material_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Collection_Filter', 'propertyName': 'collection_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Season_Filter', 'propertyName': 'season_filter'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Short_Description', 'propertyName': 'description'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Long_Description', 'propertyName': 'long_description'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Collection', 'propertyName': 'collection'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Theme_Season', 'propertyName': 'theme_season'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Color', 'propertyName': 'color'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Made_In', 'propertyName': 'made_in'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Materials', 'propertyName': 'materials'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Assembly_Instructions', 'propertyName': 'assembly_instructions'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Weight', 'propertyName': 'weight'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Dimensions', 'propertyName': 'dimensions'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Protective_Qualities', 'propertyName': 'protective_qualities'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Care_Instructions', 'propertyName': 'care_instructions'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Sports_League', 'propertyName': 'sports_league'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Sports_Team', 'propertyName': 'sports_team'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Artists', 'propertyName': 'artists'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Collection_Ad_Copy', 'propertyName': 'collection_ad_copy'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Battery_Details', 'propertyName': 'battery_details'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Battery_Included', 'propertyName': 'battery_included'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Microwave_Safe', 'propertyName': 'microwave_safe'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Capacity', 'propertyName': 'capacity'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Bullet_Points', 'propertyName': 'bullet_points'},
    {'columnObjectTypeId': '0-7', 'columnName': 'UPC', 'propertyName': 'upc'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Piece_Count_Addon', 'propertyName': 'piece_count_addon'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Retail_Box_Dimensions', 'propertyName': 'retail_box_dimensions'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Retail_Box_Weight', 'propertyName': 'retail_box_weight'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Ships_By_Truck', 'propertyName': 'ships_by_truck'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Packaging_Description', 'propertyName': 'packaging_description'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Warehouse', 'propertyName': 'warehouse'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Expedited', 'propertyName': 'expedited'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Total_Pieces_in_Program', 'propertyName': 'total_pieces_in_program'},
    {'columnObjectTypeId': '0-7', 'columnName': 'SEKeywords', 'propertyName': 'se_keywords'},
    {'columnObjectTypeId': '0-7', 'columnName': 'SEDescription', 'propertyName': 'se_description'},
    {'columnObjectTypeId': '0-7', 'columnName': 'SETitle', 'propertyName': 'se_title'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Old_Evergreen_Brands', 'propertyName': 'old_evergreen_brands'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Inventory_Now', 'propertyName': 'inventory_now'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Inventory_30_days', 'propertyName': 'inventory_30_days'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Inventory_60_days', 'propertyName': 'inventory_60_days'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Inventory_90_days_plus', 'propertyName': 'inventory_90_days'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Availability_Date', 'propertyName': 'availability_date'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Product_Status', 'propertyName': 'product_status'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Block_from_additional_discounts_promotions', ignored: true},
    {'columnObjectTypeId': '0-7', 'columnName': 'Program_Flag', 'propertyName': 'program_flag'},
    {'columnObjectTypeId': '0-7', 'columnName': 'HTS_Code', 'propertyName': 'hts_code'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Related_Products', 'propertyName': 'related_products'},
    {'columnObjectTypeId': '0-7', 'columnName': 'predefinedImage', 'propertyName': 'predefined_image'},
    {'columnObjectTypeId': '0-7', 'columnName': 'line3Text', 'propertyName': 'line_3_text'},
    {'columnObjectTypeId': '0-7', 'columnName': 'line2Text', 'propertyName': 'line_2_text'},
    {'columnObjectTypeId': '0-7', 'columnName': 'line1Text', 'propertyName': 'line_1_text'},
    {'columnObjectTypeId': '0-7', 'columnName': 'foreground', 'propertyName': 'foreground'},
    {'columnObjectTypeId': '0-7', 'columnName': 'customizableSku', 'propertyName': 'customizable_sku'},
    {'columnObjectTypeId': '0-7', 'columnName': 'customImage', 'propertyName': 'custom_image'},
    {'columnObjectTypeId': '0-7', 'columnName': 'customFont', 'propertyName': 'custom_font'},
    {'columnObjectTypeId': '0-7', 'columnName': 'background', 'propertyName': 'background'},
    {'columnObjectTypeId': '0-7', 'columnName': 'IsCustomized', 'propertyName': 'is_customized'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Style', 'propertyName': 'style'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Occasion', 'propertyName': 'occasion'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Sub_Theme', 'propertyName': 'sub_theme'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Sub_Brands', 'propertyName': 'sub_brands'},
    {'columnObjectTypeId': '0-7', 'columnName': 'Flag_4_LowStock', 'propertyName': 'flag_for_low_stock'}],
  "CM": [
    {'columnObjectTypeId': '0-2', 'columnName': 'Account_Number', 'propertyName': 'account_number', 'columnType': 'HUBSPOT_ALTERNATE_ID'},
    {'columnObjectTypeId': '0-2', 'columnName': 'EPI_Website_Number', 'propertyName': 'epi_website_number'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Corporate_Account_ID', 'propertyName': 'corporate_account_id'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Linked_ID', 'propertyName': 'linked_id_non_unique'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Sales_Rep_ID', 'propertyName': 'sales_rep_id', 'columnType': 'HUBSPOT_ALTERNATE_ID'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Market', 'propertyName': 'market'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Role', 'propertyName': 'role'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Tax_ID', 'propertyName': 'tax_id_text'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Company_Name', 'propertyName': 'name'},
    {'columnObjectTypeId': '0-1', 'columnName': 'Contact_First_Name', 'propertyName': 'firstname'},
    {'columnObjectTypeId': '0-1', 'columnName': 'Contact_Last_Name', 'propertyName': 'lastname'},
    {'columnObjectTypeId': '0-1', 'columnName': 'Contact_Email_Address', 'propertyName': 'email', 'columnType': 'HUBSPOT_ALTERNATE_ID'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Contact_Billing_Email', 'propertyName': 'contact_billing_email'},
    {'columnObjectTypeId': '0-1', 'columnName': 'Contact_Phone_Number', 'propertyName': 'phone'},
    {'columnObjectTypeId': '0-1', 'columnName': 'Contact_Phone_Number_Extension', 'propertyName': 'phone_number_extension'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Store_Type', 'propertyName': 'store_type'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Zone_ID', 'propertyName': 'zone_id', 'columnType': 'HUBSPOT_ALTERNATE_ID' },
    {'columnObjectTypeId': '0-2', 'columnName': 'Website', 'propertyName': 'domain'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Year_To_Date_Order_Total', 'propertyName': 'year_to_date_order_total'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Last_Year_Order_Total', 'propertyName': 'last_year_order_total'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Hard_Coded_Discount_Percentage', 'propertyName': 'hard_coded_discount'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Payment_Terms', 'propertyName': 'payment_terms'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Carrier', 'propertyName': 'carrier'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Carrier_Account_Number', 'propertyName': 'carrier_account_number'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Packing_List_Pricing', 'propertyName': 'packing_list_pricing'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Invoice_Receipt_Options', 'propertyName': 'invoice_receipt_options'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Special_Instructions', 'propertyName': 'special_instructions'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_First_Name', 'propertyName': 'billing_first_name'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Last_Name', 'propertyName': 'billing_last_name'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Address_1', 'propertyName': 'billing_address_1'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Address_2', 'propertyName': 'billing_address_2'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Suite', 'propertyName': 'billing_suite'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_City_APO_AFO', 'propertyName': 'billing_city_apo_afo'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_State_Province_Region', 'propertyName': 'billing_state_province_region'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Zip_Postal_Code', 'propertyName': 'billing_zip_postal_code'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Billing_Country', 'propertyName': 'billing_country'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_First_Name', 'propertyName': 'shipping_first_name'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_Last_Name', 'propertyName': 'shipping_last_name'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_Address_1', 'propertyName': 'shipping_address_1'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_Address_2', 'propertyName': 'shipping_address_2'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_Suite', 'propertyName': 'shipping_suite'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_City_APO_AFO', 'propertyName': 'shipping_city_apo_afo'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_State_Province_Region', 'propertyName': 'shipping_state_province_region'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipping_Zip_Postal_Code', 'propertyName': 'shipping_zip_postal_code'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Shipping_Country', 'propertyName': 'shipping_country'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Booked_YTD', 'propertyName': 'booked_ytd'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Credit_Limit', 'propertyName': 'credit_limit'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Credit_Amount', 'propertyName': 'credit_amount'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Past_Due_Balance', 'propertyName': 'past_due_balance'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Attention', 'propertyName': 'attention'},
    {'columnObjectTypeId': '0-2', 'columnName': 'No_Orders_Past_12', 'propertyName': 'no_orders_past_12'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Last_Order_Amount', 'propertyName': 'last_order_amount'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Last_Order_Date', 'propertyName': 'last_order_date'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Booked_Last_Year', 'propertyName': 'booked_last_year'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Credit_Amount_Last_Year', 'propertyName': 'credit_amount_last_year'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Last_Shipped_Date', 'propertyName': 'last_shipped_date'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Active', 'propertyName': 'active'},
    {'columnObjectTypeId': '0-2', 'columnName': 'Open_Not_Due', 'propertyName': 'open_not_due'},
    {'columnObjectTypeId': '0-2', 'columnName': 'RepEmail', 'propertyName': 'rep_email'}],
  "SalesRep": [
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Sales_Rep_ID', 'propertyName': 'sales_rep_id'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Market', 'propertyName': 'market'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'First_Name', 'propertyName': 'first_name'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Last_Name', 'propertyName': 'last_name'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Email_Address', 'propertyName': 'email_address'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Phone_Number', 'propertyName': 'phone_number'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Active_Customers', 'propertyName': 'active_customers'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Booked_MTD', 'propertyName': 'booked_mtd'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Booked_MTD_Goal', 'propertyName': 'booked_mtd_goal'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Booked_YTD', 'propertyName': 'booked_ytd'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Booked_YTD_Goal', 'propertyName': 'booked_ytd_goal'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipped_MTD', 'propertyName': 'shipped_mtd'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipped_MTD_Goal', 'propertyName': 'shipped_mtd_goal'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipped_YTD', 'propertyName': 'shipped_ytd'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipped_YTD_Goal', 'propertyName': 'shipped_ytd_goal'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'District_Manager_ID', 'propertyName': 'district_manager_id'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Shipping_Zip_Postal_Code', 'propertyName': 'shipping_zip_postal_code'},
    {'columnObjectTypeId': '2-43986593', 'columnName': 'Zone_ID', 'propertyName': 'zone_id', 'columnType': 'HUBSPOT_ALTERNATE_ID'}],
  "TMZip": [
    {'columnObjectTypeId': '2-43986596', 'columnName': 'Zip_Code', 'propertyName': 'zip_code', 'columnType': 'HUBSPOT_ALTERNATE_ID'},
    {'columnObjectTypeId': '2-43986596', 'columnName': 'Sales_Rep_ID', 'propertyName': 'sales_rep_id'},
    {'columnObjectTypeId': '2-43986596', 'columnName': 'Custom2', 'propertyName': 'custom2'}]
}

/**
 * List all files in GCS for this batch:
 *   - names look like `${base}____YYYY-MM-DD.txt` or with `___partN` before .txt
 * Groups them by base and ensures each base has >=1 file.
 * Returns array of filenames including suffixes.
 */
// Helper: list & group all split files for a batch
async function discoverBatchFiles(batchNum, runId) {

  console.log('Discovering Batch Files:', batchNum)
  console.log('--------------------------')
  
  const bases = BATCH_FILES[batchNum];
  const dateSuffix = runId; // e.g. "2025-05-23"
  const prefix = 'uploads/';

  const [files] = await storage.bucket(BUCKET_NAME).getFiles({ prefix });
  const groups = bases.reduce((acc, b) => ({ ...acc, [b]: [] }), {});
  
  for (const f of files) {
    const name = f.name.replace(prefix, '');
    bases.forEach(base => {
      if (name.startsWith(`${base}____${dateSuffix}`) && name.endsWith('.txt')) {
        groups[base].push(name);
        console.log('File Name:', name)
      }
    });
  }

  /*const missing = bases.filter(b => groups[b].length === 0);
  if (missing.length) {
    throw new Error(`Missing files for base: ${missing.join(', ')}`);
  }*/

  return Object.values(groups).flat();
}

// Helper: perform multipart/form-data import to HubSpot
async function createHubSpotImport(runId, batchNum, filenames) {
  let lastImportId = null;

  console.log('--------------------------');
  console.log(`Creating ${filenames.length} separate imports for batch${batchNum}`);

  for (const fn of filenames) {
    console.log(`→ Importing file ${fn}`);
    const base = fn.split('____')[0];

    // Build a fresh FormData for this file
    const form = new FormData();
    form.append('importRequest', JSON.stringify({
      name: `Import ${runId} - batch${batchNum} - ${fn}`,
      files: [{
        fileName: fn,
        fileFormat: 'CSV',
        fileImportPage: {
          hasHeader: true,
          columnMappings: FILE_SCHEMA[base]
        }
      }]
    }), { contentType: 'application/json' });

    // Attach only this one file
    const stream = storage.bucket(BUCKET_NAME)
      .file(`uploads/${fn}`)
      .createReadStream();
    form.append('files', stream, { filename: fn, contentType: 'text/csv' });

    try {
      const resp = await axios.post(HUBSPOT_UPLOAD, form, {
        headers: {
          ...form.getHeaders(),
          Authorization: `Bearer ${HUBSPOT_API_KEY}`
        },
        maxContentLength: Infinity,
        maxBodyLength:    Infinity,
      });
      lastImportId = resp.data.id;
      console.log(`  → Success, importId=${lastImportId}`);
    } catch (err) {
      console.error(`  ✖ Failed importing ${fn}:`, err.response?.data || err.message);
      // decide here if you want to throw or continue with next file:
      throw err;
    }
  }

  console.log(`Finished batch${batchNum}, lastImportId=${lastImportId}`);
  return lastImportId;
}

// Main entrypoint
(async () => {

  console.log('Initializing')
  
  try {
    
    // 1) Determine runId (today's date)
    const runId  = new Date().toISOString().slice(0,10);
    const runRef = firestore.collection(RUNS_COLLECTION).doc(runId);
  
    // 2) Attempt to read the doc
    let snap = await runRef.get();
    let batchNum;
  
    if (!snap.exists) {
      // Doc doesn’t exist: initialize it for batch 1
      batchNum = 1;
      console.log(`Run ${runId} not found—creating for batch 1`);
      await runRef.set({
        createdAt: FieldValue.serverTimestamp(),
        currentBatch: batchNum
      });
    } else {
      const data = snap.data();
      // If currentBatch is missing or falsy, default to 1
      if (!data.currentBatch) {
        batchNum = 1;
        console.log(`currentBatch missing—setting to 1 for run ${runId}`);
        await runRef.update({ currentBatch: batchNum });
      } else {
        batchNum = data.currentBatch;
        console.log(`Found run ${runId} at batch ${batchNum}`);
      }
    }

    // 3) Use currentBatch to discover files & mappings
    const baseFiles = BATCH_FILES[batchNum];
    const batchKey = `batch${batchNum}`

    // 4) discover files
    const filenames = await discoverBatchFiles(batchNum, runId);

    // 5) init Firestore doc
    await runRef.set({
      createdAt: FieldValue.serverTimestamp(),
      currentBatch: batchNum,
      [`batches.${batchKey}.status`]: 'pending',
      [`batches.${batchKey}.files`]: filenames
    }, { merge: true });

    // 6) call HubSpot import
    const importId = await createHubSpotImport(runId, batchNum, filenames);

    // 7) mark in_progress
    await runRef.update({
      [`batches.${batchKey}.importId`]: importId,
      [`batches.${batchKey}.status`]: 'in_progress'
    });

    console.log(`✔ Launched batch${batchNum} (importId: ${importId})`);
    process.exit(0);

  } catch (err) {
    console.error('❌ Importer job failed:', err);
    process.exit(1);
  }
})();
