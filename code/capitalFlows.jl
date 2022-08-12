#import Pkg; Pkg.add("Tables", "XLSX", "ExcelReaders", "DataFrames",
#       "JuMP", "Ipopt", "NamedArrays")

include("concordance.jl")


using DataFrames, JuMP, Ipopt, DelimitedFiles;

#Change GFCF_By_Industry_Asset to 20 sector
flows97 = ExcelReaders.readxlsheet("data"*pathmark*"flow1997.xls", 
    "180x22Combined");
flows = DataFrame(flows97[4:182, 4:25], :auto);
dataTypeEx = flows[41,4]
for i in [1:1:179;]
    for j in [1:1:22;]
        if typeof(flows[i,j]) == typeof(dataTypeEx)
            flows[i,j] = Float64(0.0)
        end
    end
end



# Aggregating to 20 sector
# Combining all manufacturing into the same sector
flows.x5 = flows.x5+flows.x6+flows.x7;
flows = select!(flows, Not(:x6));
flows = select!(flows, Not(:x7));
# Combining Transportation and Warehouses into the one sector
flows.x10 = flows.x10 + flows.x11;
flows = select!(flows, Not(:x11));
# Combining "Professional and technical services" and "Management of companies 
# and enterprises" into the one sector
flows.x15 = flows.x15 + flows.x16;
flows = select!(flows, Not(:x16));
n = ncol(flows) + 1;
insertcols!(flows, n, :T => zeros(length(flows.x1)));

print(flows)

# Renaming to Aus 19 Sectors codes
#= Because we begin with the 180x22 flows table, and combined a few above, we 
have 18 industries. This below is simply mapping these from the 18 combined 
industries into the Aus 19 industries categories (public sector is missing). 
This is done below by hand since it is so few sectors. It is effectively the 
concordance. =#
rename!(flows, :x1 => :A)
rename!(flows, :x2 => :B)
rename!(flows, :x3 => :D)
rename!(flows, :x4 => :E)
rename!(flows, :x5 => :C)
rename!(flows, :x8 => :F)
rename!(flows, :x9 => :G)
rename!(flows, :x10 => :I)
rename!(flows, :x12 => :J)
rename!(flows, :x13 => :K)
rename!(flows, :x14 => :L)
rename!(flows, :x15 => :M)
rename!(flows, :x17 => :N)
rename!(flows, :x18 => :P)
rename!(flows, :x19 => :Q)
rename!(flows, :x20 => :R)
rename!(flows, :x21 => :H)
rename!(flows, :x22 => :S)

rowCode = flows97[4:182,2];
containsSpace = findall( x -> occursin(" ", x), rowCode);
rowCode[containsSpace] = replace.(rowCode[containsSpace], " " => "");
rowCode20 = Array{Union{Nothing, String}}(nothing, length(rowCode));
for i in eachindex(rowCode);
    rowCode20[i] = Comm180To20[rowCode[i]];
end
insertcols!(flows ,1, :Industry => rowCode20);
insertcols!(flows ,15, :O => zeros(179));
splitIndustry = groupby(flows, :Industry);
flows = combine(splitIndustry, valuecols(splitIndustry) .=> sum);

push!(flows, ["A" zeros(1, ncol(flows) - 1)])
push!(flows, ["D" zeros(1, ncol(flows) - 1)])
push!(flows, ["H" zeros(1, ncol(flows) - 1)])
push!(flows, ["K" zeros(1, ncol(flows) - 1)])
push!(flows, ["N" zeros(1, ncol(flows) - 1)])
push!(flows, ["P" zeros(1, ncol(flows) - 1)])
push!(flows, ["Q" zeros(1, ncol(flows) - 1)])
push!(flows, ["R" zeros(1, ncol(flows) - 1)])
push!(flows, ["S" zeros(1, ncol(flows) - 1)])




# Bring in GFCF data from excel
ausGFCFall = ExcelReaders.readxlsheet(
  "data"*pathmark*"5204064_GFCF_By_Industry_Asset.xls", "Data1"
);
ausGFCFall[1, 1] = "top corner";
# remove columns
ausGFCFall = ausGFCFall[:,
  Not(findall(x -> occursin("ALL INDUSTRIES ;", x), string.(ausGFCFall[1,:])))
];
# select current totals
ausGFCFcurrent = ausGFCFall[:,
  findall(x -> 
          occursin("Gross fixed capital formation: Current", x)
          | occursin("Dwellings: Current", x)
          | occursin("transfer costs: Current", x),
    string.(ausGFCFall[1,:]))
];
# Select dates column
ausGFCFdates = ausGFCFall[:,1];
ausGFCFcurrent = hcat(ausGFCFdates, ausGFCFcurrent);
ausGFCFrow= ausGFCFcurrent[
  findall(x -> occursin("2007", x), string.(ausGFCFcurrent[:,1])), :];
#ausGFCFrow[length(ausGFCFrow) - 1] = (ausGFCFrow[length(ausGFCFrow) - 1]
#                                      + ausGFCFrow[length(ausGFCFrow)]);
ausGFCFrow = ausGFCFrow[Not(1, length(ausGFCFrow))];

#==============================================================================
wrangle the IO table
==============================================================================#
# what year are we importing?
fyend = "2007"
# Import IO data
iotable8 = DataFrame(CSV.File("data"*pathmark*fyend*pathmark*"table8.csv"));
iotable5 = DataFrame(CSV.File("data"*pathmark*fyend*pathmark*"table5.csv"));
# instantiate a variable for the number of sectors
numsec = 0
occursin("111", string(iotable8[5, 1])) ? numsec = 111 : numsec = 114
# since numsec is an array
numsec = numsec[1];
# find the title row index
titlerow = findall(x -> occursin("USE", x), string.(iotable8[:,2]));
# since titlerow is of type array and we only need its value
titlerow = titlerow[1]
# check the two tables have the same year
commonwealthcell = titlerow + 2 + numsec + 17
(iotable8[commonwealthcell, 2] != iotable5[commonwealthcell, 2]
 ? println("WARNING: io table release years don't match!")
 : println("io table release years match"))
# standardise the table
numcol = numsec + 12;
iotable8 = iotable8[:, 1:numcol];
# create column titles
coltitles = collect(values(iotable8[titlerow, :]))
coltitles[2] = "industry"
morecoltitles = collect(values(iotable8[titlerow + 2,
                                        numsec + 3 : numsec + 12]))
coltitles[numsec + 3 : numsec + 12] = morecoltitles
coltitles = filter.(x -> !isspace(x), coltitles)
coltitles[1] = "IOIG"
for i in [1:1:numsec;]
  coltitles[i + 2] = "IOIG"*values(coltitles[i + 2])
end
rename!(iotable8, string.(coltitles), makeunique=true)
# remove initial rows and tidy up
iotable8 = iotable8[Not(range(1, titlerow + 2)), :]
iotable8 = dropmissing(iotable8, :industry)
iotable8 = iotable8[Not(findall(x -> occursin("Commonwealth", x),
                                string.(iotable8.industry))),:]
# make sure our ioig codes match the number of columns  
ioigcodes = iotable8.IOIG[1:numsec]
ioigcodes = parse.(Float64, ioigcodes)
ioigto20 = map20ioig(ioigcodes)
# make a dict mapping the ioig to anzsic 20
ioigAs20=Array{Union{Nothing, String}}(nothing, length(ioigto20));
for i in eachindex(ioigcodes);
    ioigAs20[i] = ioigto20[ioigcodes[i]]
end
# convert to numbers
iotable8[:, 3:numcol] = filter.(x -> !isspace(x), iotable8[:, 3:numcol])
# Isolate GFCF
Q3 = parse.(Float64, iotable8.Q3)
Q4 = parse.(Float64, iotable8.Q4)
Q5 = parse.(Float64, iotable8.Q5)

ioigGfcf = Q3 + Q4 + Q5;
ausprodrow = findfirst(x -> occursin("Australian Production", x),
                  string.(iotable8.industry));
T1row = findfirst(x -> occursin("T1", x),
                  string.(iotable8.IOIG));
ioigGfcftot = ioigGfcf[ausprodrow];
ioigGfcf = ioigGfcf[1 : numsec] / ioigGfcf[T1row] * ioigGfcftot;
ioigGfcf = DataFrame(:inv => ioigGfcf);
# Grouping GFCF receivable by 20 sectors (dwellings is all zero in capital)
insertcols!(ioigGfcf, 1, :anzcode => ioigAs20);
splitIndustry = groupby(ioigGfcf, :anzcode);
anzdivgfcf = combine(splitIndustry, valuecols(splitIndustry) .=> sum);
sort!(anzdivgfcf);

# Balancing row and column sums to IO table 8 total
ausGFCFtot = sum(ausGFCFrow);
for i in eachindex(ausGFCFrow)
    ausGFCFrow[i] = ausGFCFrow[i] * ioigGfcftot / ausGFCFtot;
end;
# pull in proportionalised kapital flows to ras
y = DataFrame(CSV.File("data/propd-to-ras.csv"))
y = Matrix(y)
# generate an initial table for the ras
#y = zeros(length(ausGFCFrow), length(anzdivgfcf.inv_sum))
#for i in eachindex(ausGFCFrow), j in eachindex(anzdivgfcf);
#  y[i, j] = anzdivgfcf[i] / ioigGfcftot * ausGFCFrow[j]
#end  

#==============================================================================
Make prior scaled from Aus Data
==============================================================================#
# Make vector of the proportion of each row sum element as a fraction of the 
# total
rowSumsProps = ones(20);
for i in 1:20
    rowSumsProps[i] = ausGFCFrow[i] / ausGFCFtot;
end
# Make prior
ausPropPrior = ones(20, 20);
for i in 1:20
    for j in 1:20
        ausPropPrior[i,j] = rowSumsProps[j] *  anzdivgfcf.inv_sum[i];
    end
end

#==============================================================================
Make prior from US Data
==============================================================================#

print(flows)
# want to sort all rows by industry in order
# need to exclude title column after that step


#==============================================================================
RAS
==============================================================================#
# Begin Optimisation
modCap = Model(Ipopt.Optimizer);
# Should be equal dimensions
@variable(modCap, x[1:length(anzdivgfcf.inv_sum),
                    1:length(ausGFCFrow)] >= 0);
# Max entropy objective (or min relative to uniform)
@NLobjective(modCap,
             Min,
             sum((x[i, j] - y[i, j]) ^ 2
                 for i in eachindex(anzdivgfcf.inv_sum),
                 j in eachindex(ausGFCFrow)
                )
            );

# Row-sums constraint - must be equal to the GFCF totals
for j in eachindex(ausGFCFrow)
    @constraint(modCap, sum(x[:,j]) == ausGFCFrow[j]);
end;
# Col-sums constraint - must be equal to the IO totals
for k in eachindex(anzdivgfcf.inv_sum)
    @constraint(modCap, sum(x[k,:]) == anzdivgfcf.inv_sum[k]);
end;
optimize!(modCap);

# Add titles etc. for ease of reading -J works but this is clumsy fix 
ySol2 = DataFrame(value.(x), ANZSICDivShort);
insertcols!(ySol2, 1, :Divisions => ANZSICDivShort);

# Export tables as CSV
CSV.write("data"*pathmark*"capitalFlowsRAS2.csv", ySol2);
CSV.write("data"*pathmark*"UScapitalFlows.csv", flows);
