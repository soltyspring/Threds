"""Reviewable, backed-up revisions of future text reservations; never publishes."""
import argparse
import hashlib
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import threads_schedule as s
from threads_emoji_poster import BASE_URL
from competitor_formats import build_posts

ROOT=Path(__file__).resolve().parent
PLAN=ROOT/'runtime'/'competitor_plan.json'
TOPUP_PLAN=ROOT/'runtime'/'competitor_topup_plan.json'


def digest(rows):
    return hashlib.sha256(json.dumps([dict(r) for r in rows],sort_keys=True,ensure_ascii=False).encode()).hexdigest()


def known_texts(db):
    """Return current and historically scheduled texts so replacements stay genuinely new."""
    texts={r['text'] for r in db.execute('SELECT text FROM jobs') if r['text']}
    has_revisions=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='campaign_revisions'").fetchone()
    if not has_revisions:
        return texts
    for row in db.execute('SELECT previous_queue,plan_json FROM campaign_revisions'):
        for field in row:
            try:
                payload=json.loads(field)
            except (TypeError,json.JSONDecodeError):
                continue
            entries=payload if isinstance(payload,list) else payload.get('posts',[])
            texts.update(item.get('text') for item in entries if isinstance(item,dict) and item.get('text'))
    return texts


def prepare(db, current):
    rows=[dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY id')]
    future=[r for r in rows if r['state']=='pending' and datetime.fromisoformat(r['due'])>=current+timedelta(minutes=10)]
    future.sort(key=lambda r:r['due'])
    if not future: raise ValueError('No future reservations')
    if any(r['state']=='processing' for r in rows): raise ValueError('Publishing is in progress')
    generated=build_posts(len(future),exclude_texts=known_texts(db))
    return {'phase':'competitor-v3-'+current.strftime('%Y%m%dT%H%M%SZ'),
            'created_at':current.isoformat(),'account':s.EXPECTED_USER,
            'queue_digest':digest(rows),'source':'research/competitor_features_2026-09-26.json',
            'retire_pending_ids':[r['id'] for r in rows if r['state']=='pending' and r not in future],
            'blocker_ids':[r['id'] for r in rows if r['state'] in ('uncertain','failed')],
            'posts':[dict(p,job_id=r['id'],due=r['due']) for p,r in zip(generated,future)]}


def apply(db, plan, current, backup_root, reconciliation):
    # No transaction remains open from planning/API checks.
    backup=backup_root/plan['phase']
    backup.mkdir(parents=True,exist_ok=False)
    with closing(sqlite3.connect(backup/'schedule.sqlite3')) as dest: db.backup(dest)
    (backup/'plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
    db.execute('BEGIN IMMEDIATE')
    try:
        rows=[dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY id')]
        if digest(rows)!=plan['queue_digest']: raise ValueError('Queue changed; create a fresh plan')
        if any(datetime.fromisoformat(p['due'])<=current+timedelta(minutes=2) for p in plan['posts']):
            raise ValueError('Plan is stale; create a fresh plan')
        # JSON object keys are strings; normalize them before comparing against
        # the integer SQLite job ids stored in the plan.
        reconciliation = {int(job_id): evidence for job_id, evidence in reconciliation.items()}
        if set(reconciliation)!=set(plan['blocker_ids']): raise ValueError('Blocker evidence required')
        db.execute('''CREATE TABLE IF NOT EXISTS campaign_revisions (
            phase TEXT PRIMARY KEY, applied_at TEXT, previous_queue TEXT,
            previous_experiments TEXT, plan_json TEXT, reconciliation_json TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS experiment_jobs (
            job_id INTEGER PRIMARY KEY, campaign TEXT NOT NULL, metadata TEXT NOT NULL)''')
        old_exp=[dict(r) for r in db.execute('SELECT * FROM experiment_jobs')]
        db.execute('INSERT INTO campaign_revisions VALUES(?,?,?,?,?,?)',(
            plan['phase'],current.isoformat(),json.dumps(rows,ensure_ascii=False),
            json.dumps(old_exp,ensure_ascii=False),json.dumps(plan,ensure_ascii=False),
            json.dumps(reconciliation,ensure_ascii=False)))
        for job_id in plan['retire_pending_ids']:
            db.execute("UPDATE jobs SET state='expired',updated=? WHERE id=? AND state='pending'",(current.isoformat(),job_id))
        for job_id,evidence in reconciliation.items():
            if evidence.get('status') not in ('FINISHED','ERROR','EXPIRED','PUBLISHED'):
                raise ValueError('Container state is unresolved')
            # Deliberately not retried. Save the API evidence and keep this attempted post out of new content.
            db.execute("UPDATE jobs SET state='archived_'||state,updated=? WHERE id=? AND state IN ('failed','uncertain')",(current.isoformat(),job_id))
        for post in plan['posts']:
            db.execute("UPDATE jobs SET theme=?,text=?,updated=? WHERE id=? AND state='pending'",
                       (post['theme'],post['text'],current.isoformat(),post['job_id']))
            metadata={k:v for k,v in post.items() if k not in ('job_id','due')}
            db.execute('''INSERT INTO experiment_jobs(job_id,campaign,metadata) VALUES(?,?,?)
                       ON CONFLICT(job_id) DO UPDATE SET campaign=excluded.campaign,metadata=excluded.metadata''',
                       (post['job_id'],plan['phase'],json.dumps(metadata,ensure_ascii=False)))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return str(backup)


def prepare_topup(db, current, total_hours=168):
    """Plan only the missing hourly slots after the current pending queue."""
    rows=[dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY id')]
    if any(r['state'] in ('processing','uncertain','failed') for r in rows):
        raise ValueError('Active publish blocker; resolve it before extending the queue')
    pending=[r for r in rows if r['state']=='pending']
    pending.sort(key=lambda r:r['due'])
    if not pending:
        raise ValueError('No pending reservation to extend')
    if len(pending)>=total_hours:
        raise ValueError('Queue already covers the requested hourly horizon')
    need=total_hours-len(pending)
    last_due=datetime.fromisoformat(pending[-1]['due'])
    generated=build_posts(need,exclude_texts=known_texts(db))
    phase='competitor-v3-topup-'+current.strftime('%Y%m%dT%H%M%SZ')
    posts=[]
    for i,post in enumerate(generated,1):
        posts.append(dict(post,due=(last_due+timedelta(hours=i)).isoformat()))
    return {'phase':phase,'created_at':current.isoformat(),'account':s.EXPECTED_USER,
            'queue_digest':digest(rows),'target_pending_hours':total_hours,
            'existing_pending_hours':len(pending),'source':'research/competitor_features_2026-09-26.json',
            'blocker_ids':[],'posts':posts}


def apply_topup(db, plan, current, backup_root):
    """Append missing hourly slots without changing existing job ids or due times."""
    backup=backup_root/plan['phase']
    backup.mkdir(parents=True,exist_ok=False)
    with closing(sqlite3.connect(backup/'schedule.sqlite3')) as dest: db.backup(dest)
    (backup/'plan.json').write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
    db.execute('BEGIN IMMEDIATE')
    try:
        rows=[dict(r) for r in db.execute('SELECT * FROM jobs ORDER BY id')]
        if digest(rows)!=plan['queue_digest']: raise ValueError('Queue changed; create a fresh plan')
        if any(datetime.fromisoformat(p['due'])<=current+timedelta(minutes=2) for p in plan['posts']):
            raise ValueError('Plan is stale; create a fresh plan')
        if any(r['state'] in ('processing','uncertain','failed') for r in rows):
            raise ValueError('Active publish blocker; refusing to extend the queue')
        db.execute('''CREATE TABLE IF NOT EXISTS campaign_revisions (
            phase TEXT PRIMARY KEY, applied_at TEXT, previous_queue TEXT,
            previous_experiments TEXT, plan_json TEXT, reconciliation_json TEXT)''')
        db.execute('''CREATE TABLE IF NOT EXISTS experiment_jobs (
            job_id INTEGER PRIMARY KEY, campaign TEXT NOT NULL, metadata TEXT NOT NULL)''')
        old_exp=[dict(r) for r in db.execute('SELECT * FROM experiment_jobs')]
        db.execute('INSERT INTO campaign_revisions VALUES(?,?,?,?,?,?)',(
            plan['phase'],current.isoformat(),json.dumps(rows,ensure_ascii=False),
            json.dumps(old_exp,ensure_ascii=False),json.dumps(plan,ensure_ascii=False),json.dumps({},ensure_ascii=False)))
        next_id=max((r['id'] for r in rows),default=0)+1
        for offset,post in enumerate(plan['posts']):
            job_id=next_id+offset
            db.execute('''INSERT INTO jobs(id,due,theme,text,state,updated)
                       VALUES(?,?,?,?, 'pending', ?)''',
                       (job_id,post['due'],post['theme'],post['text'],current.isoformat()))
            metadata={k:v for k,v in post.items() if k!='due'}
            db.execute('''INSERT INTO experiment_jobs(job_id,campaign,metadata) VALUES(?,?,?)
                       ON CONFLICT(job_id) DO UPDATE SET campaign=excluded.campaign,metadata=excluded.metadata''',
                       (job_id,plan['phase'],json.dumps(metadata,ensure_ascii=False)))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return str(backup)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['plan','apply','topup-plan','topup-apply'])
    args=parser.parse_args()
    with closing(s.connect()) as db:
        if args.command=='plan':
            plan=prepare(db,s.now_utc())
            PLAN.write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
            print(json.dumps({'posts':len(plan['posts']),'retire_pending':len(plan['retire_pending_ids']),
                  'first':plan['posts'][0]['due'],'last':plan['posts'][-1]['due'],'phase':plan['phase']}))
        elif args.command=='apply':
            plan=json.loads(PLAN.read_text(encoding='utf-8'))
            token,_=s.token_and_profile()
            evidence={}
            for job_id in plan['blocker_ids']:
                job=db.execute('SELECT * FROM jobs WHERE id=?',(job_id,)).fetchone()
                if not job or not job['container_id']: raise ValueError('Missing container evidence')
                response=requests.get(f"{BASE_URL}/{job['container_id']}",
                                      params={'fields':'id,status'},headers={'Authorization':f'Bearer {token}'},timeout=30)
                response.raise_for_status()
                evidence[job_id]={'status':response.json().get('status'),'container_id':job['container_id'],
                                  'checked_at':s.now_utc().isoformat(),'action':'archived_without_retry'}
            backup=apply(db,plan,s.now_utc(),ROOT/'runtime'/'backups',evidence)
            s.export_json(db)
            print(json.dumps({'applied':len(plan['posts']),'backup':backup,'evidence':evidence}))
        elif args.command=='topup-plan':
            plan=prepare_topup(db,s.now_utc())
            TOPUP_PLAN.write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
            print(json.dumps({'posts':len(plan['posts']),'existing_pending':plan['existing_pending_hours'],
                  'target_pending':plan['target_pending_hours'],'first':plan['posts'][0]['due'],
                  'last':plan['posts'][-1]['due'],'phase':plan['phase']}))
        else:
            plan=json.loads(TOPUP_PLAN.read_text(encoding='utf-8'))
            token,_=s.token_and_profile()
            backup=apply_topup(db,plan,s.now_utc(),ROOT/'runtime'/'backups')
            s.export_json(db)
            print(json.dumps({'appended':len(plan['posts']),'backup':backup}))


if __name__=='__main__':
    try: main()
    except Exception as exc:
        print('ERROR: '+type(exc).__name__+'; credentials omitted')
        raise SystemExit(1)
