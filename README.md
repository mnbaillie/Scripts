Welcome to my script repository! This will contain public pre- and post-processing scripts that I've created for groundwater-surface water models.

 - sfr_routing_qc.py is intended to provide QC and a spatially-agnostic routing visualization of the stream network. This can be used on any MODFLOW model that uses SFR.
 - sfr_zonebudget.py generates a zonal surface water budget using the routing information in the SFR file, a user-supplied zone file, and the reach-by-reach streamflow file. This script can currently only be used with MODFLOW-OWHM models that use the DBFILE option in the SFR file.

If you are here, I likely talked to you about SFR ZoneBudget at the 2026 California Water Environment Modeling Forum (CWEMF) meeting. The poster I presented is included in this repository. I encourage you to test it out on your own models and see how it works for you. I am interested to see new functionalities for the script and how it can be applied to different modeling platforms.

If you have any questions about the tools here, find issues or errors in them, or want to suggest upgrades, please reach out to me at mbaillie@westyost.com or mnbaillie@gmail.com.
