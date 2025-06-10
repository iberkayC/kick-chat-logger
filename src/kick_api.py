"""
This module provides functions for interacting with the Kick API.
"""

import logging
from typing import Dict, Optional, Union
from dataclasses import dataclass
import curl_cffi
from config import KICK_API_V2_URL

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10


@dataclass
class ApiResult:
    """
    Result object for API calls.
    """

    success: bool
    data: Optional[Union[Dict, int]] = None
    error: Optional[str] = None
    status_code: Optional[int] = None


def _handle_channel_response(response, channel_name: str) -> ApiResult:
    """
    Handle channel info API response with all status codes.

    Args:
        response: The HTTP response object
        channel_name (str): The name of the channel

    Returns:
        ApiResult: Result object containing channel data or error information
    """
    if response.status_code == 200:
        try:
            data = response.json()
            logger.info("Successfully retrieved channel info for %s", channel_name)
            return ApiResult(success=True, data=data, status_code=200)
        except curl_cffi.exceptions.JSONDecodeError as e:
            logger.error("Failed to parse JSON for channel %s: %s", channel_name, e)
            return ApiResult(
                success=False,
                error=f"Invalid JSON response: {e}",
                status_code=response.status_code,
            )

    elif response.status_code == 403:
        logger.warning("Request blocked by Cloudflare for channel %s", channel_name)
        return ApiResult(
            success=False,
            error="Request blocked by Cloudflare",
            status_code=403,
        )

    elif response.status_code == 404:
        logger.info("Channel %s not found", channel_name)
        return ApiResult(
            success=False,
            error=f"Channel '{channel_name}' not found",
            status_code=404,
        )

    else:
        logger.warning(
            "Unexpected status code %d for channel %s",
            response.status_code,
            channel_name,
        )
        return ApiResult(
            success=False,
            error=f"Unexpected status code: {response.status_code}",
            status_code=response.status_code,
        )


def _handle_viewers_response(response, livestream_id: int) -> ApiResult:
    """
    Handle viewer count API response with all status codes.

    Args:
        response: The HTTP response object
        livestream_id (int): The ID of the livestream

    Returns:
        ApiResult: Result object containing viewer count or error information
    """
    if response.status_code == 200:
        try:
            data = response.json()
            if data and len(data) > 0:
                viewers = data[0].get("viewers", 0)
                logger.debug("Livestream %s has %d viewers", livestream_id, viewers)
                return ApiResult(success=True, data=viewers, status_code=200)
            else:
                logger.info("Livestream %s not found in response", livestream_id)
                return ApiResult(
                    success=False,
                    error=f"Livestream {livestream_id} not found",
                    status_code=200,
                )
        except (curl_cffi.exceptions.JSONDecodeError, IndexError, KeyError) as e:
            logger.error(
                "Failed to parse viewer count for livestream %s: %s", livestream_id, e
            )
            return ApiResult(
                success=False,
                error=f"Invalid JSON response: {e}",
                status_code=response.status_code,
            )

    elif response.status_code == 403:
        logger.warning("Request blocked by Cloudflare for livestream %s", livestream_id)
        return ApiResult(
            success=False,
            error="Request blocked by Cloudflare",
            status_code=403,
        )

    elif response.status_code == 404:
        logger.info("Livestream %s not found", livestream_id)
        return ApiResult(
            success=False,
            error=f"Livestream {livestream_id} not found",
            status_code=404,
        )

    else:
        logger.warning(
            "Unexpected status code %d for livestream %s",
            response.status_code,
            livestream_id,
        )
        return ApiResult(
            success=False,
            error=f"Unexpected status code: {response.status_code}",
            status_code=response.status_code,
        )


def get_channel_info(channel_name: str, timeout: int = DEFAULT_TIMEOUT) -> ApiResult:
    """
    Get information about a channel from the Kick API.

    Args:
        channel_name (str): The name of the channel to get information about
        timeout (int): Request timeout in seconds

    Returns:
        ApiResult: Result object containing channel data or error information
    """
    url = KICK_API_V2_URL + channel_name

    try:
        logger.info("Getting info for channel %s", channel_name)

        response = curl_cffi.get(url, impersonate="chrome", timeout=timeout)

        return _handle_channel_response(response, channel_name)

    except curl_cffi.exceptions.Timeout:
        logger.warning("Request timed out for channel %s", channel_name)
        return ApiResult(success=False, error="Request timed out")


# # not used, might be useful later
# def get_current_viewers_with_result(
#     livestream_id: int, timeout: int = DEFAULT_TIMEOUT
# ) -> ApiResult:
#     """
#     Get the current number of viewers for a livestream with detailed result.

#     Args:
#         livestream_id (int): The ID of the livestream
#         timeout (int): Request timeout in seconds

#     Returns:
#         ApiResult: Result object containing viewer count or error information
#     """
#     url = f"https://kick.com/current-viewers?ids[]={livestream_id}"

#     try:
#         logger.debug("Getting viewers for livestream %s", livestream_id)

#         response = curl_cffi.get(url, impersonate="chrome", timeout=timeout)

#         return _handle_viewers_response(response, livestream_id)

#     except curl_cffi.exceptions.Timeout:
#         logger.warning("Request timed out for livestream %s", livestream_id)
#         return ApiResult(success=False, error="Request timed out")
