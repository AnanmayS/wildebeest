import sys, time, threading, requests, psycopg, statistics, subprocess
rate_jobs=float(sys.argv[1]); per=8; dur=20
s=requests.Session(); ids=[]; lock=threading.Lock()
def submit():
    r=s.post("http://localhost:3000/jobs/synthetic",json={"count":per,"stage":"detect"},timeout=60); 
    with lock: ids.append(r.json()["jobId"])
from concurrent.futures import ThreadPoolExecutor
ex=ThreadPoolExecutor(48); t0=time.time(); n=0
stats=subprocess.Popen("docker stats --format '{{.Name}} {{.CPUPerc}}' | grep -E 'coordinator|postgres' ", shell=True, stdout=subprocess.PIPE, text=True)
while time.time()-t0<dur:
    due=int((time.time()-t0)*rate_jobs)
    while n<due: ex.submit(submit); n+=1
    time.sleep(0.01)
ex.shutdown(wait=True); sub_el=time.time()-t0
time.sleep(1)
db=psycopg.connect("postgresql://wildebeest:wildebeest@localhost:15432/wildebeest")
deadline=time.time()+60
while time.time()<deadline:
    left=db.execute("select count(*) from jobs where id = any(%s) and status='running'",(ids,)).fetchone()[0]
    if left==0: break
    time.sleep(1)
lat=[r[0] for r in db.execute("select extract(epoch from (i.finalized_at - j.created_at))*1000 from images i join jobs j on j.id=i.job_id where j.id = any(%s) and i.finalized_at is not null",(ids,))]
cpu=[]
stats.terminate()
lat.sort()
print(f"rate {rate_jobs*per:.0f} tasks/s: {len(ids)} jobs submitted in {sub_el:.1f}s, still running {left}, latency p50 {statistics.median(lat):.0f} ms p95 {lat[int(len(lat)*.95)]:.0f} ms")
