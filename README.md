# SyncSurvey - synchronize a Feature Service 

This script will extract the data from a Feature Service and import it into a File Geodatabase or an Enterprise Geodatabase. This includes preserving attachments and other relationships. On subsequent runs, only new records are imported (based on time of syncronization).

### Requirements
- The Feature Service to be synchronized must have [Sync Cababilities enabled](http://doc.arcgis.com/en/arcgis-online/share-maps/manage-hosted-layers.htm#ESRI_SECTION2_C1D5C1A8F6084949B8C5BB444F0F44EC)
- This script was designed for the Python version installed with ArcGIS Desktop 10.4.  The script can work with ArcGIS 10.3, but some modules (like tz) may need to be installed manually.

### Usage
There are three methods of use:
- ArcGIS Script tool.  This provides a graphical interface to the tool using ArcGIS Desktop.  If you have an enterprise login, you'll need to use this method through sigining into ArcGIS Online or your Portal for ArcGIS with Desktop.
- Command line - specify the parameters.  The usage in this case is:
`python syncSurvey.py <SDE Connection File | File Geodatabase> <Table Prefix> <Feature Service Url> <Time Zone> <Portal> <Username> [Password]`
The parameters have the following definition:
  - SDE Connection File | File Geodatabase: Either a `.sde` file that connects to the destination database or a File Geodatabase
  - Table Prefix - The tables are created will have the prefix applied before the name of the table (i.e., if the prefix is 's123', a table in the service with the name 'myform' will be imported as 's123_myform').  This is to prevent the chance of accidentally overwriting tables of the same name.
  - Feature Service Url - the url to the Feature Service endpoint. This is the endpoint for the Service, not an individual layer (i.e., it will end with '/FeatureServer' with no numbers)
  - Time Zone - Dates in ArcGIS Online are stored in UTC time; python processes by default without a time zone.  The time zone is needed to calculate the difference.  The time zones are specificed in (this list)[https://en.wikipedia.org/wiki/List_of_tz_database_time_zones]
  - Portal - either 'https://www.arcgis.com' for ArcGIS Online or the Portal's url
  - Username - your username for ArcGIS Online / Portal
  - Password - your password for ArcGIS Online / Portal. If you don't supply it, you'll be prompted by the script after it starts
- Using a config file.  The parameters above can be stored in a text file and read by the program; which can make automated syncronization jobs easier. Look at `config file template.txt` To run in this mode, use the following syntax:
`python syncSurvey.py CONFIG <path to config file> <section name>`