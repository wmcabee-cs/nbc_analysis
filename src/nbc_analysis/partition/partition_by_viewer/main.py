from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.transforms import merge_video_ends


def main(config_f, df):
    config = get_config(config_f=config_f)

    retval(config)
