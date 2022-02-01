/* For general debugging purposes */

#include "../perf.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>

#include "cache.h"
#include "color.h"
#include "event.h"
#include "debug.h"
#include "util.h"
#include "target.h"

int verbose;
bool dump_trace = false, quiet = false;

int eprintf(int level, const char *fmt, ...)
{
	va_list args;
	int ret = 0;

	if (verbose >= level) {
		va_start(args, fmt);
		if (use_browser >= 1)
			ui_helpline__vshow(fmt, args);
		else
			ret = vfprintf(stderr, fmt, args);
		va_end(args);
	}

	return ret;
}

int dump_printf(const char *fmt, ...)
{
	va_list args;
	int ret = 0;

	if (dump_trace) {
		va_start(args, fmt);
		ret = vprintf(fmt, args);
		va_end(args);
	}

	return ret;
}

void trace_event(union perf_event *event)
{
	unsigned char *raw_event = (void *)event;
	const char *color = PERF_COLOR_BLUE;
	int i, j;

	if (!dump_trace)
		return;

	printf(".");
	color_fprintf(stdout, color, "\n. ... raw event: size %d bytes\n",
		      event->header.size);

	for (i = 0; i < event->header.size; i++) {
		if ((i & 15) == 0) {
			printf(".");
			color_fprintf(stdout, color, "  %04x: ", i);
		}

		color_fprintf(stdout, color, " %02x", raw_event[i]);

		if (((i & 15) == 15) || i == event->header.size-1) {
			color_fprintf(stdout, color, "  ");
			for (j = 0; j < 15-(i & 15); j++)
				color_fprintf(stdout, color, "   ");
			for (j = i & ~15; j <= i; j++) {
				color_fprintf(stdout, color, "%c",
					      isprint(raw_event[j]) ?
					      raw_event[j] : '.');
			}
			color_fprintf(stdout, color, "\n");
		}
	}
	printf(".\n");
}
