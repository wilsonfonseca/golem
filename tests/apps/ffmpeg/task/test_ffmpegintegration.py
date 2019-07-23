import logging
import os

import pytest
from ffmpeg_tools.codecs import VideoCodec
from ffmpeg_tools.formats import Container
from ffmpeg_tools.validation import UnsupportedVideoCodec
from parameterized import parameterized

from apps.transcoding.common import TranscodingTaskBuilderException, \
    ffmpegException
from golem.testutils import TestTaskIntegration, \
    remove_temporary_dirtree_if_test_passed
from golem.tools.ci import ci_skip
from tests.apps.ffmpeg.task.ffmpeg_integration_base import \
    FfmpegIntegrationBase, CODEC_CONTAINER_PAIRS_TO_TEST

logger = logging.getLogger(__name__)


def create_split_and_merge_with_codec_change_test_name(
        testcase_func, _param_num, param):
    source_video_codec = {param[0][0]['video_codec'].value}
    destination_video_codec = {param[0][1].value}
    destination_container = {param[0][2].value}

    return f'{testcase_func.__name__}_{_param_num}_from_{source_video_codec}_' \
        f'to_video_codec_{destination_video_codec}_and_container' \
        f'_{destination_container}'


def create_split_and_merge_with_resolution_change_test_name(
        testcase_func, _param_num, param):
    source_width = f"{param[0][0]['resolution'][0]}"
    source_height = f"{param[0][0]['resolution'][1]}"
    destination_resolution = f"{param[0][1][0]}x{param[0][1][1]}"
    return f'{testcase_func.__name__}_{_param_num}_from_{source_width}x' \
        f'{source_height}_to_{destination_resolution}'


def create_split_and_merge_with_frame_rate_change_test_name(
        testcase_func, _param_num, param):
    source_video_codec = f"{param[0][0]['video_codec'].value}"
    source_video_container = f"{param[0][0]['container'].value}"
    destination_frame_rate = f"{str(param[0][1]).replace('/', '_')}"
    return f'{testcase_func.__name__}_{_param_num}_of_codec_' \
        f'{source_video_codec}_and_container_{source_video_container}_to_' \
        f'{destination_frame_rate}_fps'


def create_split_and_merge_with_different_subtask_counts_test_name(
        testcase_func, _param_num, param):
    source_video_codec = f"{param[0][0]['video_codec'].value}_"
    source_video_container = f"{param[0][0]['container'].value}"
    number_of_subtasks = f"{param[0][1]}_subtasks"
    return f'{testcase_func.__name__}_{_param_num}_of_codec_' \
        f'{source_video_codec}_and_container_{source_video_container}_into_' \
        f'{number_of_subtasks}_subtasks' \


@ci_skip
class TestFfmpegIntegration(FfmpegIntegrationBase):

    # flake8: noqa
    # pylint: disable=line-too-long
    VIDEO_FILES = [
        {"resolution": [320, 240], "container": Container.c_MP4, "video_codec": VideoCodec.H_264,     "key_frames": 1,     "path": "test_video.mp4"},
        {"resolution": [320, 240], "container": Container.c_MP4, "video_codec": VideoCodec.H_264,     "key_frames": 2,     "path": "test_video2"},
    ]
    # pylint: enable=line-too-long

    @classmethod
    def _create_task_def_for_transcoding(  # pylint: disable=too-many-arguments
            cls,
            resource_stream,
            result_file,
            container,
            video_options=None,
            subtasks_count=2,
    ):
        task_def_for_transcoding = {
            'type': 'FFMPEG',
            'name': os.path.splitext(os.path.basename(result_file))[0],
            'timeout': '0:10:00',
            'subtask_timeout': '0:09:50',
            'subtasks_count': subtasks_count,
            'bid': 1.0,
            'resources': [resource_stream],
            'options': {
                'output_path': os.path.dirname(result_file),
                'video': video_options if video_options is not None else {},
                'container': container,
            }
        }

        return task_def_for_transcoding

    @parameterized.expand(
        (
            (video, video_codec, container)
            for video in VIDEO_FILES  # pylint: disable=undefined-variable
            for video_codec, container in CODEC_CONTAINER_PAIRS_TO_TEST
        ),
        name_func=create_split_and_merge_with_codec_change_test_name
    )
    @pytest.mark.slow
    @remove_temporary_dirtree_if_test_passed
    def test_split_and_merge_with_codec_change(self,
                                               video,
                                               video_codec,
                                               container):
        super().split_and_merge_with_codec_change(video, video_codec, container)

    @parameterized.expand(
        (
            (video, resolution)
            for video in VIDEO_FILES  # pylint: disable=undefined-variable
            for resolution in (
                [400, 300],
                [640, 480],
                [720, 480],
            )
        ),
        name_func=create_split_and_merge_with_resolution_change_test_name
    )
    @pytest.mark.slow
    @remove_temporary_dirtree_if_test_passed
    def test_split_and_merge_with_resolution_change(self, video, resolution):
        super().split_and_merge_with_resolution_change(video, resolution)

    @parameterized.expand(
        (
            (video, frame_rate)
            for video in VIDEO_FILES  # pylint: disable=undefined-variable
            for frame_rate in (1, 25, '30000/1001', 60)
        ),
        name_func=create_split_and_merge_with_frame_rate_change_test_name
    )
    @pytest.mark.slow
    @remove_temporary_dirtree_if_test_passed
    def test_split_and_merge_with_frame_rate_change(self, video, frame_rate):
        super().split_and_merge_with_frame_rate_change(video, frame_rate)

    @parameterized.expand(
        (
            (video, subtasks_count)
            for video in VIDEO_FILES
            for subtasks_count in (1, 6, 10, video['key_frames'])
        ),
        name_func=create_split_and_merge_with_different_subtask_counts_test_name
    )
    @pytest.mark.slow
    @remove_temporary_dirtree_if_test_passed
    def test_split_and_merge_with_different_subtask_counts(self,
                                                           video,
                                                           subtasks_count):
        super().\
            split_and_merge_with_different_subtask_counts(video, subtasks_count)

    @remove_temporary_dirtree_if_test_passed
    def test_simple_case(self):
        resource_stream = os.path.join(self.RESOURCES, 'test_video2')
        result_file = os.path.join(self.root_dir, 'test_simple_case.mp4')
        task_def = self._create_task_def_for_transcoding(
            resource_stream,
            result_file,
            container=Container.c_MP4.value,
            video_options={
                'codec': 'h265',
                'resolution': [320, 240],
                'frame_rate': "25",
            })

        task = self.execute_task(task_def)
        result = task.task_definition.output_file
        self.assertTrue(TestTaskIntegration.check_file_existence(result))

    @remove_temporary_dirtree_if_test_passed
    def test_nonexistent_output_dir(self):
        resource_stream = os.path.join(self.RESOURCES, 'test_video2')
        result_file = os.path.join(self.root_dir, 'nonexistent', 'path',
                                   'test_invalid_task_definition.mp4')
        task_def = self._create_task_def_for_transcoding(
            resource_stream,
            result_file,
            container=Container.c_MP4.value,
            video_options={
                'codec': 'h265',
                'resolution': [320, 240],
                'frame_rate': "25",
            })

        task = self.execute_task(task_def)

        result = task.task_definition.output_file
        self.assertTrue(TestTaskIntegration.check_file_existence(result))
        self.assertTrue(TestTaskIntegration.check_dir_existence(
            os.path.dirname(result_file)))

    @remove_temporary_dirtree_if_test_passed
    def test_nonexistent_resource(self):
        resource_stream = os.path.join(self.RESOURCES,
                                       'test_nonexistent_video.mp4')

        result_file = os.path.join(self.root_dir, 'test_nonexistent_video.mp4')
        task_def = self._create_task_def_for_transcoding(
            resource_stream,
            result_file,
            container=Container.c_MP4.value,
            video_options={
                'codec': 'h265',
                'resolution': [320, 240],
                'frame_rate': "25",
            })

        with self.assertRaises(TranscodingTaskBuilderException):
            self.execute_task(task_def)

    @remove_temporary_dirtree_if_test_passed
    def test_invalid_resource_stream(self):
        resource_stream = os.path.join(
            self.RESOURCES,
            'invalid_test_video.mp4')
        result_file = os.path.join(self.root_dir,
                                   'test_invalid_resource_stream.mp4')

        task_def = self._create_task_def_for_transcoding(
            resource_stream,
            result_file,
            container=Container.c_MP4.value,
            video_options={
                'codec': 'h265',
                'resolution': [320, 240],
                'frame_rate': "25",
            })

        with self.assertRaises(ffmpegException):
            self.execute_task(task_def)

    @remove_temporary_dirtree_if_test_passed
    def test_task_invalid_params(self):
        resource_stream = os.path.join(self.RESOURCES, 'test_video2')
        result_file = os.path.join(self.root_dir, 'test_invalid_params.mp4')
        task_def = self._create_task_def_for_transcoding(
            resource_stream,
            result_file,
            container=Container.c_MP4.value,
            video_options={
                'codec': 'abcd',
                'resolution': [320, 240],
                'frame_rate': "25",
            })

        with self.assertRaises(UnsupportedVideoCodec):
            self.execute_task(task_def)
