from pyx import *

g = graph.graphxy(width=8, x=graph.axis.bar(),y=graph.axis.logarithmic())
g.plot(graph.data.file("dedisbenchdist", x=1, y=2), [graph.style.bar()])
g.writeEPSfile("dedisbenchhist")
g.writePDFfile("dedisbenchhist")

h = graph.graphxy(width=8, x=graph.axis.bar(),y=graph.axis.logarithmic())
h.plot(graph.data.file("dupsdist", x=1, y=2), [graph.style.bar()])
h.writeEPSfile("dupsdisthist")
h.writePDFfile("dupsdisthist")