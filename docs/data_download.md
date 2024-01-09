# Documentation and Scenario Dataset Versions

The zip files below contain the `Documentation` and `Scenario` directories.

Current release version:
- [FTOT 2023_4.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2023_4.zip)

Previous releases:
- [FTOT 2023_3.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2023_3.zip)
- [FTOT 2023_2.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2023_2.zip)
- [FTOT 2023_1.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2023_1.zip)
- [FTOT_2022_4.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2022_4.zip) - final version compatible with previous specification of FTOT US multimodal network
- ~FTOT_2022_3.zip~ _Deprecated_
- ~FTOT_2022_2.zip~ _Deprecated_
- ~FTOT_2022_1.zip~ _Deprecated_
- ~FTOT_2021_4.zip~ _Deprecated_
- ~FTOT_2021_3.zip~ _Deprecated_
- ~FTOT_2021_2.zip~ _Deprecated_
- ~FTOT_2021_1.zip~ _Deprecated_
- [FTOT_2020_4.1.zip](https://www.volpe.dot.gov/our-work/FTOT/FTOT_2020_4_1.zip) - final version compatible with ArcGIS 10.x and Python 2
- ~FTOT_2020_4.zip~ _Deprecated_
- ~FTOT_2020_3.zip~ _Deprecated_
- ~FTOT_2020_2.zip~ _Deprecated_
- ~FTOT_2020_1.zip~ _Deprecated_
- ~FTOT_2019_4.zip~ _Deprecated_
- ~FTOT_2019_3.zip~ _Deprecated_

### Troubleshooting tip: Rename the file with a .zip extension if necessary. 

**Note for 2022.4 release:** If you downloaded supporting documentation and scenario data corresponding to the FTOT 2022.4 release **prior to February 1, 2023**, there was an issue with the Tableau dashboard that causes processor facilities to show up too small on the Supply Chain Summary map. To download a fixed Tableau workbook, complete the following steps:
1. Go to [this page](https://www.volpe.dot.gov/our-work/FTOT/tableau_dashboard.twb). The browser will open a page with raw XML text for the new workbook.
2. In Windows Explorer, locate the **old** tableau_dashboard.twb file in your scenarios/common_data folder. Right click the .twb file and open in a text editor.
3. **Replace all text** in the old .twb file with the raw XML text from the browser-based .twb file. Save and close the file.
You can now run FTOT as usual.

## North American Multimodal Network
The FTOT Team has created a North American multimodal network by adding available Canadian and Mexican modal data to FTOT’s default contiguous U.S. multimodal network. The draft network includes road, rail, waterway and intermodal facility data for Canada, along with rail and road data for Mexico. The network is designed to facilitate North American scenarios with a scope beyond the continental United States. If you would like to request a copy from the FTOT Team, please email Kristin Lewis (<Kristin.Lewis@dot.gov>). More details are available in Appendices B and C of the Technical Documentation.

# Next Steps
Return to the [FTOT homepage](https://volpeusdot.github.io/FTOT-Public) to complete the installation.
