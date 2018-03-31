import numpy as np
import matplotlib.pyplot as plt

x = [1,2,3,4,5]
y = [4,7,4,7,3]


y2 = [5,3,2,6,2]


plt.plot(x, y , label='Initial Line')
plt.plot(x, y2, label='New line' )


plt.xlabel('Plot Number')
plt.ylabel('Random Number')
plt.title('Epic Graph tutorial for data vis in Python with Matplotlib. \nTutorial showing labels and titles')

plt.legend()

plt.show()
