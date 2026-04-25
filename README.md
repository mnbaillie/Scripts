Welcome to my script repository! This will contain public pre- and post-processing scripts that I've created for groundwater-surface water models.

 - sfr_routing_qc.py is intended to provide QC and a spatially-agnostic routing visualization of the stream network. This can be used on any MODFLOW model that uses SFR.
 - sfr_zonebudget.py generates a zonal surface water budget using the routing information in the SFR file, a user-supplied zone file, and the reach-by-reach streamflow file. This script can currently only be used with MODFLOW-OWHM models that use the DBFILE option in the SFR file.

Routing visualizations produced by these scripts use the Graphviz DOT language (https://graphviz.org/doc/info/lang.html). If the user does not have the Graphviz executable installed, the .dot file produced by the script can be copied to https://dreampuf.github.io/GraphvizOnline/?engine=dot to generate an image of the routing network.

If you are here, I likely talked to you about SFR ZoneBudget at the 2026 California Water Environment Modeling Forum (CWEMF) meeting. The poster I presented is included in this repository. I encourage you to test it out on your own models and see how it works for you. I am interested to see new functionalities for the script and how it can be applied to different modeling platforms.

If you have any questions about the tools here, find issues or errors in them, or want to suggest upgrades, please reach out to me at mbaillie@westyost.com or mnbaillie@gmail.com.

References:
 - MODFLOW Streamflow-Routing Package v2 (SFR2): Niswonger, R.G. and Prudic, D.E., 2005, Documentation of the Streamflow-Routing (SFR2) Package to include unsaturated flow beneath streams--A modification to SFR1, U.S. Geological Survey Techniques and Methods 6-A13, 47 p.
 - MODFLOW Lake Package (LAK): Merritt, M.L. and Konikow, L.F., 2000, Documentation of a Computer Program to Simulate Lake-Aquifer Interaction Using the MODFLOW Ground-Water Flow Model and the MOC3D Solute-Transport Model, U.S. Geological Survey Water-Resources Investigations Report 00-4167, 146 p.
 - Graphviz Dot Language: Gansner, E.R. and S.C. North, 1999, An Open Graph Visualization System and Its Applications to Software Engineering, Software-Practice and Experience, 00(S1), p.1-5.
