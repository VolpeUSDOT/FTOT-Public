import arcpy
import os
from six.moves import input

# THIS SCRIPT IS USED TO AGGREGATE GRID CELL PRODUCTION DATA, E.G., FBEP OR USDA, BY COUNTY
countyLyr = r"C:\FTOT\scenarios\common_data\base_layers\cb_2017_us_county_500k.shp"


# ==============================================================================

def get_user_input():
    print("start: get_user_input")
    return input(">>> input raster: ")


# ==============================================================================

def cleanup(gdb):
    print("start: cleanup")
    if arcpy.Exists(gdb):
        print("start: delete existing gdb")
        print("gdb file location: " + gdb)
        try:
            arcpy.Delete_management(gdb)
        except:
            print("could not delete " + gdb)
            raise Exception("could not delete " + gdb)


# ==============================================================================

def aggregate_raster():

    """aggregates the rasters by county"""
    print("start: aggregate_raster")
    inputRaster = get_user_input()
    outFolder = os.path.dirname(inputRaster)
    raster_name = str(os.path.basename(inputRaster))
    outGdbName = str(raster_name + ".gdb")
    tempGdb = os.path.join(outFolder, outGdbName)
    outputCounties = str(os.path.join(tempGdb,raster_name))

    cleanup(tempGdb)
    if not arcpy.Exists(tempGdb):
        arcpy.CreateFileGDB_management(outFolder, outGdbName)
    outPoints = os.path.join(tempGdb, "outPoints")
    clippedRaster = os.path.join(tempGdb, "clippedFBEP")
    joinOutput = os.path.join(tempGdb, "joinOutput")

    # 5 arc-minute is about 10 square kilometers
    sqKilometersPer5ArcMinute = 10
    hectaresPerSqKilometer = 100
    hectareArea = sqKilometersPer5ArcMinute * hectaresPerSqKilometer

    rectangle = "-124.848974 24.396308 -66.885444 49.384358"
    print("start: clip_management")
    arcpy.Clip_management(inputRaster, rectangle, clippedRaster)

    print("start: raster_to_point_conversion")
    arcpy.RasterToPoint_conversion(clippedRaster, outPoints)

    print("start: project_management")
    arcpy.Project_management(countyLyr, os.path.join(tempGdb, "counties_projected"), arcpy.Describe(outPoints).spatialReference)

    print("start: spatial_join")
    arcpy.SpatialJoin_analysis(outPoints, os.path.join(tempGdb, "counties_projected"), joinOutput, "JOIN_ONE_TO_ONE",
                               "KEEP_COMMON", "", "COMPLETELY_WITHIN")

    # countyProdDict[FIPS] = total tons
    countyProdDict = {}
    print("start: raster_output")
    print("start: ...\\create_county_production_dictionary loop")
    for row in arcpy.da.SearchCursor(joinOutput, ["GEOID", "grid_code"]):
        if row[0] not in countyProdDict:
            countyProdDict[row[0]] = row[1] * hectareArea
        else:
            countyProdDict[row[0]] += row[1] * hectareArea

    print("start: ...\\create_output_counties_fc")
    arcpy.CopyFeatures_management(countyLyr, outputCounties)

    print("start: ...\\add_fields_to_county_output_fc")
    arcpy.AddField_management(outputCounties, "Facility_Name", "TEXT")

    print("start: ...\\calculate_field_for_facility_name")
    arcpy.CalculateField_management(in_table=outputCounties, field="Facility_Name",
                                    expression="'rmp_'+!GEOID!", expression_type="PYTHON_9.3", code_block="")

    print("start: create_csv")
    filename = str("rmp_" + commodity + ".csv")
    facility_type = "raw_material_producer"
    units = "kg"
    phase_of_matter = "solid"

    a_filename = os.path.join(outFolder, filename)
    with open(a_filename, 'w') as wf:
        # write the header line
        header_line = "facility_name,facility_type,commodity,value,units,phase_of_matter,io"
        wf.write(str(header_line + "\n"))
        for key in countyProdDict:
            record = "{},{},{},{},{},{},{}".format(str("rmp_"+key), facility_type, raster_name, countyProdDict[key],
                                                   units,
                                                   phase_of_matter, 'o')
            wf.write(str(record + "\n"))

    print("start: delete_temp_layers")
    arcpy.Delete_management(outPoints)
    arcpy.Delete_management(clippedRaster)
    arcpy.Delete_management(joinOutput)
    arcpy.Delete_management(counties_projected)

    print("finish: aggregate_raster")
    return


# ==============================================================================

def run():
    aggregate_raster()

