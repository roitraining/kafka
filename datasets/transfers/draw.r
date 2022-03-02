# install.packages("igraph")

require("igraph")

# input format is V1 V2 weight
x <- read.graph("sample.txt", format="ncol", directed=TRUE)

# output to a png image ...
png(filename="sample.png", height=480, width=480)

# E(x) is the edge-graph of x.  Use weight to thicken edges
# There are lots of parameters to control the plotting.
# From within R, type ?igraph.plotting  to get help
wts = E(x)$weight
wts = 5 * (wts / max(wts))  # thickest line is 5 pixels wide
arrows = (wts > 2) # draw arrows only on thick lines

# change the colors depending on weight.
colors = rep("darkgray", length(wts)) 
colors[wts < 1] = "white"
colors[wts >= 3] = "black"

plot(x, edge.width=wts, edge.arrow.mode=arrows, edge.color=colors)
