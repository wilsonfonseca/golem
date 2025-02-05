import asyncio
import logging
import os
import os.path
import shutil
import tempfile
import unittest
from pathlib import Path
from time import sleep

import ethereum.keys
import pycodestyle

from golem.core.common import get_golem_path, is_windows, is_osx
from golem.core.simpleenv import get_local_datadir
from golem.database import Database
from golem.model import DB_MODELS, db, DB_FIELDS

logger = logging.getLogger(__name__)


class TempDirFixture(unittest.TestCase):
    root_dir = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        logging.basicConfig(level=logging.DEBUG)
        if cls.root_dir is None:
            if is_osx():
                # Use Golem's working directory in ~/Library/Application Support
                # to avoid issues with mounting directories in Docker containers
                cls.root_dir = os.path.join(get_local_datadir('tests'))
                os.makedirs(cls.root_dir, exist_ok=True)
            elif is_windows():
                import win32api  # noqa pylint: disable=import-error
                base_dir = get_local_datadir('default')
                cls.root_dir = os.path.join(base_dir, 'ComputerRes', 'tests')
                os.makedirs(cls.root_dir, exist_ok=True)
                cls.root_dir = win32api.GetLongPathName(cls.root_dir)
            else:
                # Select nice root temp dir exactly once.
                cls.root_dir = tempfile.mkdtemp(prefix='golem-tests-')

    # Concurrent tests will fail
    # @classmethod
    # def tearDownClass(cls):
    #     if os.path.exists(cls.root_dir):
    #         shutil.rmtree(cls.root_dir)

    def setUp(self):

        # KeysAuth uses it. Default val (250k+) slows down the tests terribly
        ethereum.keys.PBKDF2_CONSTANTS['c'] = 1

        prefix = self.id().rsplit('.', 1)[1]  # Use test method name
        self.tempdir = tempfile.mkdtemp(prefix=prefix, dir=self.root_dir)
        self.path = self.tempdir  # Alias for legacy tests
        if not is_windows():
            os.chmod(self.tempdir, 0o770)
        self.new_path = Path(self.path)

    def tearDown(self):
        # Firstly kill Ethereum node to clean up after it later on.
        try:
            self.__remove_files()
        except OSError as e:
            logger.debug("%r", e, exc_info=True)
            tree = ''
            for path, _dirs, files in os.walk(self.path):
                tree += path + '\n'
                for f in files:
                    tree += f + '\n'
            logger.error("Failed to remove files %r", tree)
            # Tie up loose ends.
            import gc
            gc.collect()
            # On windows there's sometimes a problem with syncing all threads.
            # Try again after 3 seconds
            sleep(3)
            self.__remove_files()

    def temp_file_name(self, name: str) -> str:
        return os.path.join(self.tempdir, name)

    def additional_dir_content(self, file_num_list, dir_=None, results=None,
                               sub_dir=None):
        """
        Create recursively additional temporary files in directories in given
        directory.
        For example file_num_list in format [5, [2], [4, []]] will create
        5 files in self.tempdir directory, and 2 subdirectories - first one will
        contain 2 tempfiles, second will contain 4 tempfiles and an empty
        subdirectory.
        :param file_num_list: list containing number of new files that should
            be created in this directory or list describing file_num_list for
            new inner directories
        :param dir_: directory in which files should be created
        :param results: list of created temporary files
        :return:
        """
        if dir_ is None:
            dir_ = self.tempdir
        if sub_dir:
            dir_ = os.path.join(dir_, sub_dir)
            if not os.path.exists(dir_):
                os.makedirs(dir_)
        if results is None:
            results = []
        for el in file_num_list:
            if isinstance(el, int):
                for _ in range(el):
                    t = tempfile.NamedTemporaryFile(dir=dir_, delete=False)
                    results.append(t.name)
            else:
                new_dir = tempfile.mkdtemp(dir=dir_)
                self.additional_dir_content(el, new_dir, results)
        return results

    def __remove_files(self):
        if os.path.isdir(self.tempdir):
            shutil.rmtree(self.tempdir)


class DatabaseFixture(TempDirFixture):
    """ Setups temporary database for tests."""

    def setUp(self):
        super(DatabaseFixture, self).setUp()
        self.database = Database(db, fields=DB_FIELDS, models=DB_MODELS,
                                 db_dir=self.tempdir)

    def tearDown(self):
        self.database.db.close()
        super(DatabaseFixture, self).tearDown()


class TestWithClient(TempDirFixture):

    def setUp(self):
        super(TestWithClient, self).setUp()
        self.client = unittest.mock.Mock()
        self.client.datadir = os.path.join(self.path, "datadir")


class PEP8MixIn(object):
    """A mix-in class that adds PEP-8 style conformance.
    To use it in your TestCase just add it to inheritance list like so:
    class MyTestCase(unittest.TestCase, testutils.PEP8MixIn):
        PEP8_FILES = <iterable>

    PEP8_FILES attribute should be an iterable containing paths of python
    source files relative to <golem root>.

    Afterwards your test case will perform conformance test on files mentioned
    in this attribute.
    """

    def test_conformance(self, *_):
        """Test that we conform to PEP-8."""
        style = pycodestyle.StyleGuide(
            ignore=pycodestyle.DEFAULT_IGNORE.split(','),
            max_line_length=80)

        # PyCharm needs absolute paths
        base_path = Path(get_golem_path())
        absolute_files = [str(base_path / path) for path in self.PEP8_FILES]

        result = style.check_files(absolute_files)
        self.assertEqual(result.total_errors, 0,
                         "Found code style errors (and warnings).")


def async_test(coro):
    def wrapper(*args, **kwargs):
        loop = asyncio.new_event_loop()
        return loop.run_until_complete(coro(*args, **kwargs))
    return wrapper
