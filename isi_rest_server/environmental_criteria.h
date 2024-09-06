#ifndef _ENVIRONMENTAL_CRITERIA_H_
#define _ENVIRONMENTAL_CRITERIA_H_

#include "request.h"

/**
 * @file functions that match uri_handler's environmental_criteria
 *       requirements for gating calls to particular papi handlers
 *       based on the request data. This is to avoid sprinkling
 *       checks all over various papi handlers, which is much more
 *       difficult to maintain and validate.
 */

/**
 * @brief Do not run particular uri_handler methods on manufacturing
 *        builds.
 * @param req - the request being evaluated.
 * @throws api_exception if it should not be run - with appropriate
 *         user text.
 */
void
no_modify_on_manufacturing(const request &req);

#endif // _ENVIRONMENTAL_CRITERIA_H_
