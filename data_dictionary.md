# Data Dictionary
- fact_i94: Built on i94 entry record from US National Tourism and Trade Office. **It can be used to join other dimension tables to count number of people enter US in different states, month, visa type etc.**

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|i94_key|integer|N||key for each i94 entry|1,2,3...|
|res_region_key|integer|NNN|3|i94 applicant's residente country code|236|
|cit_region_key|integer|NNN|3|i94 applicant's citizen country code|236|
|port_key|string|SSS|3|US port key|FMY|
|arrival_date|date|YYYY-MM-DD||Arrival Date in the USA|2016-04-11|
|i94yr|integer|YYYY|4|I94 entry year|2016|
|i94mon|integer|D||I94 entry month|0-12|
|i94mode_key|integer|D|1|I94 entry mode key|1,2,3,9|
|state_code|string|SS|2|I94 applicant's address state|CA|
|departure_date|date|YYYY-MM-DD||I94 departure date|2016-04-16|
|age|integer|DD||I94 applicant's age|0-99|
|visa_key|integer|D||Visa code|1,2,3|
|i94_count|integer|D||Number people of the i94 entry|1|
|i94_file_date|date|YYYY-MM-DD||I94 file date|2016-04-16|
|occup|string|SSS||Occupation that will be performed in U.S.|STU|
|biryear|integer|YYYY||4 digit year of birth|1950|
|i94_leave_date|date|YYYY-MM-DD||Applican's leave date on I94|2016-07-16|
|gender|string|S||Applicant's gender|F|
|insnum|string|SSS||INS number|3517|
|airline|string|SS||Airline used to arrive in U.S.|CI|
|i94_admin_num|string|||I94 Administration number|55780468433|
|fltno|string||   |Flight number of Airline used to arrive in U.S.|XBLNG|
|visatype|string|   |   |Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|B2|

- dim_port: A normalized table built from I94 immigration dictionary. It contains state, and address of each entry port.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|port_key|string|||port code|ALC|
|state|string|||state abbrev|AK|
|address|string|||port address|ALCAN|

- dim_time: A normaliaed table build from I94 entry time column. It has day, week, month, year, weekday for each arrival date.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|arrival_date|date|YYYY-MM-DD||Arrival Date in the USA|2016-04-16|
|day|integer|||arrival date|16|
|week|integer|||week of the arrival|1|
|month|integer|||month of the arrival|4|
|year|integer|||year of the arrival|2016|
|weekday|integer|||weekday|1|

- dim_i94_mode: A normaliaed table built from I94 immigration dictionary "entry mode" column.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|i94mode_key|integer|||I94 entry mode key||
|i94mode|string|||I94 entry mode|Air, Land, Sea|

- dim_visa: A normalized table built from I94 immigration dictionary "visa type" column.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|visa_key|integer||||1,2,3|
|visa_broad_type|string|||visa type|Business, Pleasure, Student|

- dim_country: A normalized table built from I94 immigration dictionary "country/region code" column.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|visa_key|integer||||1,2,3|
|visa_broad_type|string|||visa type|Business, Pleasure, Student|

- dim_state: A normalized table built from I94 immigration dictionary "state" code column.

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|state_code|string|||state abbrev|AL|
|state|string|||state name|ALABAMA|

- fact_demographics: It's built from raw US city demographics. 

|Field Name| Data Type|Data Format|Field Size|Description|Example|
|:---:|:---:|:---:|:---:|:---:|:---:|
|city|string|||city name|Silver Spring|
|state|string|||state name|Maryland|
|median_age|double|||median age|33.8|
|male_population|integer|||male population|40601|
|female_population|integer|||female population|41862|
|total_population|integer|||total population|82463|
|number_of_veterans|integer|||number of veterans|1562|
|foreign_born|integer|||foreign born population|30908|
|average_household_size|double|||average household size|3.2|
|state_code|string|||state code|MD|
|race|string||||White|
