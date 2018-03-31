import numpy as np
import matplotlib.pyplot as plt

year = [1,2,3,4,5,6,7,8,9,10]

#n in thousands
taxes = [17,18,40,43,44,8,43,32,39,30]

overhead = [30,22,9,29,17,12,14,24,49,35]

entertainment = [41,32,27,13,19,12,22,18,28,20]

plt.plot([],[],color='m', label='taxes')
plt.plot([],[],color='c', label='overhead')
plt.plot([],[],color='b', label='entertainment')


plt.stackplot(year, taxes, overhead, entertainment, colors=['m','c','b'])
plt.legend()
plt.title('company expenses')
plt.xlabel('year')
plt.ylabel('Cost in thousands')
plt.show()
