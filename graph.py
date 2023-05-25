import csv
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.gridspec as gridspec
import matplotlib as mplt
import matplotlib.ticker as plticker
import seaborn as sns


conc = "high"

result_file = f'./results/result_{conc}.csv'

mpl = [16, 32, 64, 128, 256]
txn_sizes = [150, 100, 50, 10, 1]
iso_level = ["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]

dct = {"SERIALIZABLE": {txn: {m: [] for m in mpl} for txn in txn_sizes},
       "REPEATABLE READ": {txn: {m: [] for m in mpl} for txn in txn_sizes},
       "READ COMMITTED": {txn: {m: [] for m in mpl} for txn in txn_sizes}}

# 'txn_size', 'mpl', "isolation_level", 'total_response_time', 'elapsed_time', 'num_transactions', 'avg_response', 'throughput', 'max_response', 'min_response', 'avg_transaction_delay'
with open(result_file, 'r') as f:
    reader = csv.reader(f)
    next(reader, None)
    for line in reader:
        dct[line[2]][int(line[0])][int(line[1])].append(tuple([float(line[3]), float(line[4]), int(line[5]), float(line[6]), float(line[7]),float(line[8]),float(line[9]),float(line[10])]) )

    #result_data = [tuple([int(line[0]), int(line[1]), line[2], float(line[3]), float(line[4]), int(line[5]), float(line[6]), float(line[7]),float(line[8]),float(line[9]),float(line[10])]) for line in reader]

markers = ['o', 'x', '+']
colors = ['red', 'orange', 'blue', 'green', 'purple']

seri = mlines.Line2D([], [], color='black', marker=markers[0], linestyle='None',
                          markersize=10, label='Serializable')
repr = mlines.Line2D([], [], color='black', marker=markers[1], linestyle='None',
                          markersize=10, label='Repeatable Read')
reco = mlines.Line2D([], [], color='black', marker=markers[2], linestyle='None',
                          markersize=10, label='Read committed')

s150 = mlines.Line2D([], [], color='red', label='Size = 150')
s100 = mlines.Line2D([], [], color='orange', label='Size = 100')
s50 = mlines.Line2D([], [], color='blue',label='Size = 50')
s10 = mlines.Line2D([], [], color='green', label='Size = 10')
s1 = mlines.Line2D([], [], color='purple', label='Size = 1')


fig = plt.figure( figsize=(14,9), dpi=80, facecolor='w', edgecolor='k' )
mplt.rcParams.update({'axes.labelsize': 'large', 'axes.titlesize': 'large'})
idx = 4

# gs = gridspec.GridSpec(2, 3, figure=fig)
# ax1 = fig.add_subplot(gs[0, 0])
# # identical to ax1 = plt.subplot(gs.new_subplotspec((0, 0), colspan=3))
# ax2 = fig.add_subplot(gs[0, 1])
# ax3 = fig.add_subplot(gs[0, 2])
# ax4 = fig.add_subplot(gs[1, 0])
# ax5 = fig.add_subplot(gs[1, 1])
ax1 = plt.subplot2grid(shape=(2,6), loc=(0,0), colspan=2)
ax2 = plt.subplot2grid((2,6), (0,2), colspan=2)
ax3 = plt.subplot2grid((2,6), (0,4), colspan=2)
ax4 = plt.subplot2grid((2,6), (1,1), colspan=2)
ax5 = plt.subplot2grid((2,6), (1,3), colspan=2)
ax = [ax1, ax2, ax3, ax4, ax5]
rows = 2
columns = 3
# for i in range(1,6):
#     fig.add_subplot(rows, columns, i)
# select iso_level
for i in range(3):
    # select txn_level
    txn_levels =[]
    for j in range(5):
        # collect mpl
        data = []
        for k in mpl:
            data.append(dct[iso_level[i]][txn_sizes[j]][k][0][idx]*txn_sizes[j])
        print(iso_level[i], txn_sizes[j], data)
        #ax = plt.subplot(2, 3, j + 1)
        txn_levels.append(data)

        ax[j].plot(mpl, data, color=colors[j], marker=markers[i])
        ax[j].set_xlabel("MPL")
        ax[j].set_ylabel("Query Throughput")
        ax[j].grid()
        ax[j].set_title(f"Txn Size = {txn_sizes[j]}")
        plt.tight_layout()
        loc = plticker.MultipleLocator(base=1000.0) # this locator puts ticks at regular intervals
        ax[j].yaxis.set_major_locator(loc)
        if j == 3:
            plt.legend(handles=[seri, repr, reco], loc='center left', bbox_to_anchor=(1, 0.5))
        #ax[j].tight_layout()
#plt.yscale('log')
#plt.subplots_adjust(pad=-5.0)

plt.subplots_adjust(hspace=0.2, wspace=1)
plt.show()

plt.clf()
fig = plt.figure( figsize=(8,6), dpi=80, facecolor='w', edgecolor='k' )
for i in range(3):
    # select txn_level
    txn_levels =[]
    for j in range(5):
        # collect mpl
        data = []
        for k in mpl:
            data.append(dct[iso_level[i]][txn_sizes[j]][k][0][idx]*txn_sizes[j])
        print(iso_level[i], txn_sizes[j], data)
        #ax = plt.subplot(2, 3, j + 1)
        txn_levels.append(data)

        plt.plot(mpl, data, color=colors[j], marker=markers[i])

loc = plticker.MultipleLocator(base=500.0)
plt.grid()
plt.xlabel("MPL")
plt.ylabel("Query Throughput")
plt.legend(handles=[seri, repr, reco, s150, s100, s50, s10, s1], loc='center left', bbox_to_anchor=(1, 0.5))
plt.gca().yaxis.set_major_locator(loc)
plt.subplots_adjust(hspace=0.2, wspace=1)
plt.tight_layout()
plt.show()
