Weather-Hotels data joined by 4-characters geohash.

| Column name | Description | Data type | Partition |
| --- | --- | --- | --- |
| address | Hotel address | string | no |
| avg_tmpr_c | Average temperature in Celsius | double | no |
| avg_tmpr_f | Average temperature in Fahrenheit | double | no |
| city | Hotel city | string | no |
| country | Hotel country | string | no |
| geoHash | 4-characters geohash based on Longitude & Latitude | string | no |
| id | ID of hotel | string | no |
| latitude | Latitude of a weather station | double | no |
| longitude | Longitude of a weather station | double | no |
| name | Hotel name | string | no |
| wthr_date | Date of observation (YYYY-MM-DD) | string | no |
| wthr_year | Year of observation (YYYY) | string | yes |
| wthr_month | Month of observation (MM) | string | yes |
| wthr_day | Day of observation (DD) | string | yes |
