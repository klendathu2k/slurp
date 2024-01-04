#!/usr/bin/env python

from matplotlib import pyplot as plt 
from matplotlib import dates as mdates

import numpy as np
from matplotlib.patches import StepPatch

import numpy as np
import pyodbc
import pprint
import datetime
import time

now = datetime.datetime.now()
now = datetime.datetime( now.year, now.month, now.day, now.hour, now.minute )
print(now)

# Production status ... TODO: refactor production_status table to use "ps" and "psc"... to support different DB's...
statusdb = pyodbc.connect("DSN=FileCatalog")
statusdbc = statusdb.cursor()

get_dst_event_timestamps = """
select submitting,submitted,started,running,ended,status from production_status where dsttype like 'DST_EVENT%';
"""

get_dst_calor_timestamps = """
select submitting,submitted,started,running,ended,status from production_status where dsttype like 'DST_CALOR%';
"""

dst_event_timestamps = statusdbc.execute( get_dst_event_timestamps ).fetchall()
dst_calor_timestamps = statusdbc.execute( get_dst_calor_timestamps ).fetchall()

def roundoff( t ):
    return datetime.datetime( t.year, t.month, t.day, t.hour, t.minute, t.second )

dst_event_label  = ["submitting", "submitted", "started", "running", "finished", "failed" ]
dst_event_colors = [ "mediumblue", "darkblue", "indigo", "gold", "forestgreen", "red" ]

dst_event_submiting = [ roundoff(t[0]) for t in dst_event_timestamps if t[0] ] 
dst_event_submitted = [ roundoff(t[1]) for t in dst_event_timestamps if t[1] ] 
dst_event_started   = [ roundoff(t[2]) for t in dst_event_timestamps if t[2] ] 
dst_event_running   = [ roundoff(t[3]) for t in dst_event_timestamps if t[3] ] 
dst_event_finished  = [ roundoff(t[4]) for t in dst_event_timestamps if t[4] and t[5]=='finished' ] 
dst_event_failed    = [ roundoff(t[4]) for t in dst_event_timestamps if t[4] and t[5]=='failed' ] 

dst_calor_label  = dst_event_label
dst_calor_colors = dst_event_colors

dst_calor_submiting = [ roundoff(t[0]) for t in dst_calor_timestamps if t[0] ] 
dst_calor_submitted = [ roundoff(t[1]) for t in dst_calor_timestamps if t[1] ] 
dst_calor_started   = [ roundoff(t[2]) for t in dst_calor_timestamps if t[2] ] 
dst_calor_running   = [ roundoff(t[3]) for t in dst_calor_timestamps if t[3] ] 
dst_calor_finished  = [ roundoff(t[4]) for t in dst_calor_timestamps if t[4] and t[5] == 'finished' ] 
dst_calor_failed    = [ roundoff(t[4]) for t in dst_calor_timestamps if t[4] and t[5] == 'failed' ] 

fig, axs = plt.subplots(2, 1, sharex=True)

dst_event_values, dst_event_bins, dst_event_bars = axs[0].hist( 
    [dst_event_submiting,
     dst_event_submitted,
     dst_event_started,
     dst_event_running,
     dst_event_finished,
     dst_event_failed ],
     label=dst_event_label,
     color=dst_event_colors
)


count=0
for c in axs[0].containers:
    labels = [ b if b > 0 else "" for b in c.datavalues ]
    axs[0].bar_label( c, labels=labels, fmt="%i", 
                      rotation=90, 
                      size=10, 
                      padding=10, 
                      color=dst_event_colors[count%len(dst_event_colors)] 
    )
    count=count+1
count=0

axs[0].legend(prop={'size': 10})
axs[0].set_title(f'DST_EVENT Production Status {str(now)}')


dst_calor_values, dst_calor_bins, dst_calor_bars = axs[1].hist( 
    [dst_calor_submiting,
     dst_calor_submitted,
     dst_calor_started,
     dst_calor_running,
     dst_calor_finished,
     dst_calor_failed],
     label=dst_calor_label,
     color=dst_calor_colors
)

#pprint.pprint( dst_calor_values )
#dst_calor_csum = [
#    np.cumsum( dst_calor_values[0] ),
#    np.cumsum( dst_calor_values[1] ),
#    np.cumsum( dst_calor_values[2] ),
#    np.cumsum( dst_calor_values[3] ),
#    np.cumsum( dst_calor_values[4] ),
#    np.cumsum( dst_calor_values[5] ),
#]
#pprint.pprint( dst_calor_csum )
#dst_calor_finished_sum = np.cumsum( dst_calor_values[4] )
#axs[1].plot( dst_calor_bins, dst_calor_finished_sum, alpha=0.7, color="forestgreen", label="finished sum" )

#axs[1].hist( dst_calor_finished, cumulative=True, color="forestgreen", label="sum finished" )
axs[1].hist(    
    [
        dst_calor_submiting,
        dst_calor_submitted,
        dst_calor_started,
        dst_calor_running,
        dst_calor_finished,
        dst_calor_failed
    ],
    cumulative=True,
    color=dst_calor_colors,
    alpha=0.125
)
    



count=0
for c in axs[1].containers:
    labels = [ b if b > 0 else "" for b in c.datavalues ]
    axs[1].bar_label( c, labels=labels, fmt="%i", 
                      rotation=90, 
                      size=10, 
                      padding=10, 
                      color=dst_calor_colors[count%len(dst_calor_colors)] 
    )
    count=count+1
count=0

axs[1].legend(prop={'size': 10})
axs[1].set_title(f'DST_CALOR Production Status {str(now)}')

fig.tight_layout()
plt.show( block=False )
plt.pause(120)
#time.sleep(120)















