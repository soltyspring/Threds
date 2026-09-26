import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import apply_competitor_campaign as a


class ApplyTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.db=a.s.connect(self.root/'schedule.sqlite3')
        self.now=datetime(2026,9,26,8,tzinfo=timezone.utc)
        self.db.execute('CREATE TABLE experiment_jobs(job_id INTEGER PRIMARY KEY,campaign TEXT,metadata TEXT)')
        for i,state in enumerate(['published','uncertain','pending','pending','pending'],1):
            due=self.now+timedelta(hours=i-3)
            self.db.execute('INSERT INTO jobs(id,due,theme,text,state,post_id) VALUES(?,?,?,?,?,?)',
                            (i,due.isoformat(),'old',f'old{i}',state,'post1' if i==1 else None))
            self.db.execute('INSERT INTO experiment_jobs VALUES(?,?,?)',(i,'old','{}'))
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def test_backup_history_metadata_and_no_reschedule(self):
        plan=a.prepare(self.db,self.now)
        self.assertEqual(len(plan['posts']),2)
        backup=a.apply(self.db,plan,self.now,self.root/'backups',{2:{'status':'FINISHED'}})
        self.assertEqual(self.db.execute('SELECT state,post_id FROM jobs WHERE id=1').fetchone()[:],('published','post1'))
        self.assertEqual(self.db.execute('SELECT state FROM jobs WHERE id=2').fetchone()[0],'archived_uncertain')
        self.assertEqual(self.db.execute('SELECT state FROM jobs WHERE id=3').fetchone()[0],'expired')
        self.assertEqual(self.db.execute('SELECT due FROM jobs WHERE id=4').fetchone()[0],plan['posts'][0]['due'])
        self.assertTrue((Path(backup)/'schedule.sqlite3').exists())
        self.assertEqual(len(json.loads(self.db.execute('SELECT previous_queue FROM campaign_revisions').fetchone()[0])),5)
        self.assertIn('old4',a.known_texts(self.db))

    def test_changed_queue_refuses_to_overwrite(self):
        plan=a.prepare(self.db,self.now)
        self.db.execute("UPDATE jobs SET text='user edit' WHERE id=4")
        self.db.commit()
        with self.assertRaises(ValueError): a.apply(self.db,plan,self.now,self.root/'backups',{2:{'status':'FINISHED'}})
        self.assertEqual(self.db.execute('SELECT text FROM jobs WHERE id=4').fetchone()[0],'user edit')

    def test_unresolved_container_rolls_back(self):
        plan=a.prepare(self.db,self.now)
        with self.assertRaises(ValueError): a.apply(self.db,plan,self.now,self.root/'backups',{2:{'status':'IN_PROGRESS'}})
        self.assertEqual(self.db.execute('SELECT state FROM jobs WHERE id=2').fetchone()[0],'uncertain')

    def test_topup_appends_missing_hourly_slots(self):
        db=a.s.connect(self.root/'topup.sqlite3')
        db.execute('CREATE TABLE experiment_jobs(job_id INTEGER PRIMARY KEY,campaign TEXT,metadata TEXT)')
        for i in range(1,4):
            due=self.now+timedelta(hours=i)
            db.execute('INSERT INTO jobs(id,due,theme,text,state) VALUES(?,?,?,?,?)',
                       (i,due.isoformat(),'old',f'topup-old{i}','pending'))
        db.commit()
        plan=a.prepare_topup(db,self.now,total_hours=5)
        self.assertEqual(len(plan['posts']),2)
        self.assertEqual(plan['posts'][0]['due'],(self.now+timedelta(hours=4)).isoformat())
        backup=a.apply_topup(db,plan,self.now,self.root/'backups')
        self.assertTrue((Path(backup)/'schedule.sqlite3').exists())
        self.assertEqual(db.execute('SELECT COUNT(*) FROM jobs').fetchone()[0],5)
        self.assertEqual(db.execute('SELECT MIN(id),MAX(id) FROM jobs WHERE id>3').fetchone()[:],(4,5))
        self.assertEqual(db.execute('SELECT COUNT(*) FROM experiment_jobs').fetchone()[0],2)
        db.close()


if __name__=='__main__': unittest.main()
