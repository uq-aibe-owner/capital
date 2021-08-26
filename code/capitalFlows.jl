include("concordance.jl")

using Ipopt

#Change GFCF_By_Industry_Asset to 19 sector
flows97 = ExcelReaders.readxlsheet("data"*pathmark*"flow1997.xls", "180x22Combined");
flows = DataFrame(flows97[4:182, 4:25], :auto);
dataTypeEx = flows[41,4]
for i in [1:1:179;]
    for j in [1:1:22;]
        if typeof(flows[i,j]) == typeof(dataTypeEx)
            flows[i,j] = Float64(0.0)
        end
    end
end

# Aggregating to 19 sector
flows.x5 = flows.x5+flows.x6+flows.x7;
flows = select!(flows, Not(:x6));
flows = select!(flows, Not(:x7));
flows.x10 = flows.x10+flows.x11;
flows = select!(flows, Not(:x11));
flows.x15 = flows.x15+flows.x16;
flows = select!(flows, Not(:x16));

# Renaming to 19 Sectors codes
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
rowCode19 = Array{Union{Nothing, String}}(nothing, length(rowCode));
for i in eachindex(rowCode);
    rowCode19[i] = Comm180To19[rowCode[i]];
end
insertcols!(flows ,1, :Industry => rowCode19);
insertcols!(flows ,15, :O => zeros(179));
splitIndustry = groupby(flows, :Industry);
flows = combine(splitIndustry, valuecols(splitIndustry) .=> sum);

push!(flows, ["A" zeros(1,19)])
push!(flows, ["D" zeros(1,19)])
push!(flows, ["H" zeros(1,19)])
push!(flows, ["K" zeros(1,19)])
push!(flows, ["N" zeros(1,19)])
push!(flows, ["P" zeros(1,19)])
push!(flows, ["Q" zeros(1,19)])
push!(flows, ["R" zeros(1,19)])
push!(flows, ["S" zeros(1,19)])

sort!(flows)

ausGFCF = ExcelReaders.readxlsheet("data"*pathmark*"5204064_GFCF_By_Industry_Asset.xls", "Data1");
ausGFCF = ausGFCF[:,Not(findall(x -> occursin("ALL INDUSTRIES ;", x), string.(ausGFCF[1,:])))]
ausGFCF2019 = ausGFCF[findall(x -> occursin("2019", x), string.(ausGFCF[:,1])),
    findall(x -> occursin("Gross fixed capital formation: Chain volume measures ;", x), string.(ausGFCF[1,:]))];


IOIGAs19=Array{Union{Nothing, String}}(nothing, length(IOIG));
for i in eachindex(IOIG);
    IOIGAs19[i] = IOIGTo19[IOIG[i]]
end

ausCapReceiv = DataFrame((IOSource[4:117,findall(x -> occursin("Private ;\n Gross Fixed Capital Formation", x), string.(IOSource[2,:]))]+
IOSource[4:117,findall(x -> occursin("Public Corporations ;\n Gross Fixed Capital Formation", x), string.(IOSource[2,:]))]), [:GFCF]);
insertcols!(ausCapReceiv ,1, :Industry => IOIGAs19);
splitIndustry = groupby(ausCapReceiv, :Industry);
ausCapReceiv = combine(splitIndustry, valuecols(splitIndustry) .=> sum);
sort!(ausCapReceiv);

ausCapReceivable = ausCapReceiv.GFCF_sum



#currently just going with 2019 and balancing rows and collumns, had a pretty good look and it doesnt get much closer in other years and it does get much further apart
ausCapReceivableSum = sum(ausCapReceivable)
ausGFCF2019Sum = sum(ausGFCF2019)
avSum = (ausCapReceivableSum + ausGFCF2019Sum)/2;

for i in eachindex(ausCapReceivable)
    ausCapReceivable[i]=ausCapReceivable[i]*avSum/ausCapReceivableSum;
end
for i in eachindex(ausGFCF2019)
    ausGFCF2019[i]=ausGFCF2019[i]*avSum/ausGFCF2019Sum;
end

modCap = Model(Ipopt.Optimizer);
@variable(modCap, x[1:length(ausCapReceivable), 1:length(ausGFCF2019);
@NLobjective(modCap, Min, sum(((x[i,j] - (flows[i,j]+1000))/ (flows[i,j]+1000))^ 2 for i in eachindex(ausCapReceivable), j in eachindex(ausGFCF2019)));
for i in eachindex(ausGFCF2019)
    @constraint(modCap, sum(x[:,i]) == ausGFCF2019[i]+19000;
end
for i in eachindex(ausCapReceivable);
    @constraint(modCap, sum(x[i,:]) == ausCapReceiv[i]+19000;
end

optimize!(modCap);

