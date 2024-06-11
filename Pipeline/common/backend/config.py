from typing import Any, Optional, Dict
from sqlalchemy import Connection
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm import sessionmaker, Session
from common.backend.models import Configurations


class Config:

    def __init__(self, *, project: Optional[str] = None, conn: Optional[Connection] = None):
        self._project = project or "default"
        if not conn:
            from common.utils import create_postgres_connection
            conn = create_postgres_connection()
        self._conn = conn
    
    def table(self, sess: Session):
        return sess.query(Configurations).filter_by(project=self._project)
    
    def _commit(self):
        self._conn.commit()
    
    def load_config(self) -> Dict:
        config = {}
        with Session(self._conn) as sess:
            for item in self.table(sess).all():
                config[item.key] = item.value
        return config
    
    def _get_one(self, key: str):
        try:
            with Session(self._conn) as sess:
                result = self.table(sess).filter_by(key=key).one()
            return result
        except NoResultFound:
            return None
    
    def get(self, key: str, default: Optional[Any] = None):
        item = self._get_one(key)
        if item is None:
            if default is not None:
                self.set(key, default)
            return default
        else:
            return item.value
    
    def set(self, key: str, value: Any):
        item = self._get_one(key) or Configurations(project=self._project, key=key, value=value)
        item.value = value
        with Session(self._conn) as sess:
            sess.merge(item)
            sess.commit()
        self._conn.commit()
    
    def unset(self, key: str):
        item = self._get_one(key)
        if item is not None:
            with Session(self._conn) as sess:
                sess.delete(item)
                sess.commit()
            self._conn.commit()
