# include("preptables-pretable8specific.jl")

# for the time being this should be run with pwd being the gladstone parent file

using DataFrames, CSV, DelimitedFiles;

# Directory for data output (and input while pulling template from .csv)
dataDirect = "julia\\output\\"

# Name of column that contains the row indices
sectorColName = "ANZdiv"

# Pull in tables from .csv files, ultimately it will come from the RAS code
anztable8rr = CSV.read(dataDirect * "newtable8.csv", DataFrame)
anztable5rr = CSV.read(dataDirect * "newtable5.csv", DataFrame)
# Quickly mock up anztablediffrr from tables 5 and 8, ultimately it will come 
# from the RAS code #include -d at the top of this file
anztablediffrr = select(anztable8rr, Not(sectorColName))
for i in 1:nrow(anztablediffrr)
    for j in 1:ncol(anztablediffrr)
        anztablediffrr[i,j] = (anztablediffrr[i,j] - 
                select(anztable5rr, Not(sectorColName))[i,j])
    end
end
anztablediffrr[!, sectorColName] = anztable8rr[:, sectorColName]

# List of names of sectors, ultimately it will come from the RAS code 
# include -d at the top of this file
sectorCodes = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
    "N", "O", "P", "Q", "R", "S", "T"];

#=============================================================================#
# General code to add Sector titles and convert to a df from a matrix 
# dataSet if that's what the input is (not the case right now)

# Here need to just get the data, no headings etc. in matrix form
# Comment out if they are already in this form
# anztable8rr = Matrix(anztable8rr)
# anztable5rr = Matrix(anztable5rr)
# anztablediffrr = Matrix(anztablediffrr)

# function addTitles(table)
#     df = DataFrame(table, sectorCodes);
#     insertcols!(df, 1, :Sectors => SectorCodes);
#     return df;
# end

# addTitles(anztable8rr)
# addTitles(anztable5rr)
# addTitles(anztablediffrr)

#=============================================================================#
# Function to get list of row indices for given sectors etc. Used only as an 
# input to the getSubFrame function at this stage
function getRows(dfColumn, lsOfSectors)
    rowVec = [];
    for i in 1:length(lsOfSectors)
        append!(rowVec, findall(x -> x == lsOfSectors[i], dfColumn)[1])
    end
    return rowVec
end
# General Function to get a sub-frame based on row and column names
# Makes the code for extracting each parameter much more readable
function getSubFrame(df, lsRows, lsCols);
    subdf = df[getRows(df[:,sectorColName], lsRows), append!([sectorColName],
    lsCols)]
    return subdf
end
#=============================================================================#
# Calculate and export RAW parameters as .csv files

# RAW_CON_FLW - all sectors in the Q1 col of table 8
CSV.write(dataDirect*"RAW_CON_FLW.csv", getSubFrame(anztable8rr, 
    sectorCodes, ["Q1"]));

# RAW_MED_FLW - all inter-sector flows table 8
CSV.write(dataDirect*"RAW_MED_FLW.csv", getSubFrame(anztable8rr, 
    sectorCodes, sectorCodes));

# RAW_KAP_OUT - all sectors in the P2 row of table 8
# First example of a row - hence the permutedims
CSV.write(dataDirect*"RAW_KAP_OUT.csv", permutedims( 
    getSubFrame(anztable8rr, ["`P2"], sectorCodes), 1));

# RAW_LAB_OUT all sectors in the P1 row of table 8
CSV.write(dataDirect*"RAW_LAB_OUT.csv", permutedims(
    getSubFrame(anztable8rr, ["`P1"], sectorCodes),1));

# RAW_MED_OUT - all sectors in the T1 row of table 8 (maybe use sum instead)
#=CSV.write(dataDirect*"RAW_MED_OUT.csv", permutedims(
    getSubFrame(anztable8rr, ["T1"], sectorCodes), 1);=#

# RAW_DOM_CCON - all sectors in the Q1 col of table 5
CSV.write(dataDirect*"RAW_DOM_CCON.csv", getSubFrame(anztable5rr, 
    sectorCodes, ["Q1"]));

# RAW_YSA_CCON - all sectors flows of table 8-5
CSV.write(dataDirect*"RAW_YSA_CCON.csv", getSubFrame(anztablediffrr, 
    sectorCodes, ["Q1"]));

# RAW_DOM_CINV - all sectors in the Q3+Q4+Q5 col of table 5
RAW_DOM_CINV = sum(eachcol(select(getSubFrame(anztable5rr, sectorCodes, ["Q3", 
    "Q4", "Q5"]), Not(sectorColName))))
RAW_DOM_CINV = DataFrame(RAW_DOM_CINV = RAW_DOM_CINV, sectorColName = sectorCodes)
CSV.write(dataDirect*"RAW_DOM_CINV.csv", RAW_DOM_CINV);

# RAW_YSA_CINV - all sectors flows of kapflows table - not yet imported
# CSV.write(dataDirect*"RAW_YSA_CINV.csv", getSubFrame(kapflows, 
#     sectorCodes, sectorCodes));

# RAW_DOM_CMED - all sectors flows of table 5
CSV.write(dataDirect*"RAW_DOM_CMED.csv", getSubFrame(anztable5rr, 
sectorCodes, sectorCodes));

# RAW_YSA_CMED - all sectors flows of table 8-5
CSV.write(dataDirect*"RAW_YSA_CMED.csv", getSubFrame(anztablediffrr, 
sectorCodes, sectorCodes));

# RAW_EXO_JOUT -  all sectors in the Q1 col of table 8
CSV.write(dataDirect*"RAW_EXO_JOUT.csv", getSubFrame(anztable8rr, 
    sectorCodes, ["Q7"]));

# RAW_DOM_JOUT - all sectors in the (T6 - Q7) col of table 8 -J make sure same 
# method as for RAW_DOM_CINV works, will be best to test with input data
#= CSV.write(dataDirect*"RAW_DOM_JOUT.csv", getSubFrame(anztable8rr, 
    sectorCodes, ["T6"])); =#

