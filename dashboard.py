try:
    import simplejson as json
    print "Using simplejson for faster json parsing"
except ImportError:
    import json
from datetime import datetime
import math, sys
import telemetryutils
import jydoop
import math, random

verbose = False

############################# HistogramAggregator

class HistogramAggregator:
    """ Object that accumulates a single histogram, ie. it aggregates histograms
        Format of values is
        [
            bucket0,
            bucket1,
            ...,
            bucketN,
            sum,                # -1, if missing
            log_sum,            # -1, if missing
            log_sum_squares,    # -1, if missing
            sum_squares_lo,     # -1, if missing
            sum_squares_hi,     # -1, if missing
            count
        ]
        Ie. same format as telemetry-server validated histograms with an extra
        count field.

        Notice that constructor for this object takes same values as constructed
        by the dump() method. Hence, dump = aggregator.dump() followed by
        HistogramAggregator(**dump) will restore the aggregator.
        (Notice without JSON dumps/loads this is just a shallow copy!)

        Like wise aggregators can be merged:
        aggregator1.merge(**aggregator2.dump())
    """
    def __init__(self, values = [], buildId = "", revision = None):
        self.values = values
        self.buildId = buildId
        self.revision = revision

    def merge(self, values, buildId, revision):
        # If length of values don't match up, we have two different histograms
        if len(self.values) != len(values):
            # Choose the histogram with highest buildId
            if self.buildId < buildId:
                self.values = values
                self.buildId = buildId
                self.revision = revision
        else:
            if self.buildId < buildId:
                self.values = values
                self.buildId = buildId
            for i in xrange(0, len(values) - 6):
                self.values[i] += values[i] or 0
            # Entries [-6:-1] may have -1 indicating missing entry
            for i in xrange(len(values) - 6, len(values) - 1):
                # Missing entries are indicated with -1, we shouldn't add these up
                if self.values[i] == -1 and values[i] == -1:
                    continue
                self.values[i] += values[i] or 0
            # Last entry cannot be negative
            self.values[-1] += values[-1] or 0

    def dump(self):
        return {
            'revision':     self.revision,
            'buildId':      self.buildId,
            'values':       self.values
        }

############################## Ugly hacks for simple measures

# Auxiliary method for computing bucket offsets from parameters, it is stolen
# from histogram_tools.py, though slightly modified...
def exponential_buckets(dmin, dmax, n_buckets):
    log_max = math.log(dmax);
    ret_array = [0] * n_buckets
    current = dmin
    ret_array[1] = current
    for bucket_index in range(2, n_buckets):
        log_current = math.log(current)
        log_ratio = (log_max - log_current) / (n_buckets - bucket_index)
        log_next = log_current + log_ratio
        next_value = int(math.floor(math.exp(log_next) + 0.5))
        if next_value > current:
            current = next_value
        else:
            current = current + 1
        ret_array[bucket_index] = current
    return ret_array

# Create buckets from buckets2index from ranges... snippet pretty much stolen
# from specgen.py
def buckets2index_from_ranges(ranges):
    buckets = map(str, ranges)
    bucket2index = {}
    for i in range(0, len(buckets)):
        bucket2index[buckets[i]] = i
    return bucket2index

# Bucket offsets for simple measures
simple_measures_buckets =   (
                                buckets2index_from_ranges(
                                        exponential_buckets(1, 30000, 50)
                                    ),
                                    exponential_buckets(1, 30000, 50)
                            )


############################# Jydoop Integration, data extraction

def sanitize_and_put(context, filePath, filterPath, values, buildId, revision):
    # Sanitize values
    for i in xrange(0, len(values)):
        if values[i] is None:
            values[i] = 0
        if type(values[i]) is float:
            values[i] = long(values[i])
        if type(values[i]) not in (int, long):
            return
    # Write sanitized output
    context.write(filePath, {filterPath: {
        'values':   values,
        'buildId':  buildId,
        'revision': revision
    }})

SPECS = "scripts/histogram_specs.json"
histogram_specs = json.loads(jydoop.getResource(SPECS))

def map(uid, line, context):
    global histogram_specs

    #if random.random() > 0.01:
    #    return

    payload = json.loads(line)

    submissionDate = uid[1:9] # or uid[1:9]
    try:
        i = payload['info']
        channel = i.get('appUpdateChannel', "too_old")
        OS = i['OS']
        appName = i['appName']
        reason = i['reason']
        osVersion = str(i['version'])
        #only care about major versions
        majorVersion = i['appVersion'].split('.')[0]
        arch = i['arch']
        buildId = i['appBuildID']
        buildDate = buildId[:8]
        revision = i['revision']
    except (KeyError, IndexError, UnicodeEncodeError):
        if verbose:
            msg = "error while unpacking the payload"
            print >> sys.stderr, msg
        return

    # TODO: histogram_specs should specify the list of versions/channels we
    #       care about
    if not channel in ['release', 'aurora', 'nightly', 'beta', 'nightly-ux']:
        return

    # todo combine OS + osVersion + santize on crazy platforms like linux to
    #      reduce pointless choices
    if OS == "Linux":
        osVersion = osVersion[:3]

    filterPathBD = (buildDate, reason, appName, OS, osVersion, arch)
    # Sanitize filterPath
    for val in filterPathBD:
        if not isinstance(val, basestring) and type(val) in (int, float, long):
            if verbose:
                print >> sys.stderr, "Found type %s in filterPathBD" % type(val)
            return

    filterPathSD = (submissionDate, reason, appName, OS, osVersion, arch)
    # Sanitize filterPath
    for val in filterPathSD:
        if not isinstance(val, basestring) and type(val) in (int, float, long):
            if verbose:
                print >> sys.stderr, "Found type %s in filterPathSD" % type(val)
            return

    # Sanitize channel and majorVersion
    for val in (channel, majorVersion):
        if not isinstance(val, basestring) and type(val) in (int, float, long):
            if verbose:
                print >> sys.stderr, ("Found type %s in channel or majorVersion" %
                                      type(val))
            return

    bdate = datetime.strptime(buildDate, "%Y%m%d")
    sdate = datetime.strptime(submissionDate, "%Y%m%d")
    skip_by_submission_date = True
    if (sdate - bdate).days < 60:
        skip_by_submission_date = False

    ######### dimensions and other fields are loaded and sanitized

    for h_name, h_values in payload.get('histograms', {}).iteritems():
        bucket2index = histogram_specs.get(h_name, None)
        if bucket2index is None and h_name.startswith('STARTUP_'):
            bucket2index = histogram_specs.get(h_name[8:], None)
        if bucket2index is None:
            if verbose:
                msg = "bucket2index is None in map for %s" % h_name
                print >> sys.stderr, msg
            continue
        else:
            bucket2index = bucket2index[0]

        # most buckets contain 0s, so preallocation is a significant win
        outarray = [0] * (len(bucket2index) + 6)

        index_error = False
        type_error = False
        if not isinstance(h_values, dict):
            if verbose:
                msg = "h_values is not a dictionary"
                print >> sys.stderr, msg
            continue

        try:
            values = h_values.get('values', None)
        except AttributeError:
            msg = "h_values was not a dict"
            print >> sys.stderr, msg
            continue
        if values is None:
            continue
        for bucket, value in values.iteritems():
            index = bucket2index.get(bucket, None)
            if index is None:
                #print "%s's does not feature %s bucket in schema" % (h_name, bucket)
                index_error = True
                break
            if type(value) not in (int, long, float):
                type_error = True
                if verbose:
                    print >> sys.stderr, "Bad value type: %s " % repr(value)
                break
            outarray[index] = value
        if index_error:
            if verbose:
                msg = "index is None in map"
                print >> sys.stderr, msg
            continue
        if type_error:
            if verbose:
                msg = "value is not int, long or float"
                print >> sys.stderr, msg
            continue

        histogram_sum = h_values.get('sum', None)
        if histogram_sum is None:
            if verbose:
                msg = "histogram_sum is None in map"
                print >> sys.stderr, msg
            continue
        if type(histogram_sum) not in (int, long, float):
            if verbose:
                msg = ("histogram_sum is not int, long or float, but: %s" %
                       type(histogram_sum))
                print >> sys.stderr, msg
            continue

        outarray[-6] = histogram_sum                        # sum
        outarray[-5] = h_values.get('log_sum', -1)          # log_sum
        outarray[-4] = h_values.get('log_sum_squares', -1)  # log_sum_squares
        outarray[-3] = h_values.get('sum_squares_lo', -1)   # sum_squares_lo
        outarray[-2] = h_values.get('sum_squares_hi', -1)   # sum_squares_hi
        outarray[-1] = 1                                    # count

        # Sanitize array:
        for i in xrange(0, len(outarray)):
            outarray[i] = outarray[i] or 0

        # by build date

        filePath = (channel, majorVersion, h_name, "by-build-date")

        try:
            sanitize_and_put(context, filePath, filterPathBD, outarray,
                             buildId, revision)
        except TypeError:
            dict_locations = [p for p, t in enumerate(filterPathBD) if type(t) is dict]
            if dict_locations:
                field_names = ["buildDate", "reason", "appName", "OS",
                               "osVersion", "arch"]
                dict_field_names = [field_names[i] for i in dict_locations]
                msg = ("unable to hash the following `filterPathBD` fields: %s" %
                       (' '.join(dict_field_names)))
            else:
                msg = "TypeError when writing map output."
            if verbose:
                print >> sys.stderr, msg

        # by submission date
        if skip_by_submission_date:
            continue

        filePath = (channel, majorVersion, h_name, "by-submission-date")
        try:
            sanitize_and_put(context, filePath, filterPathSD, outarray,
                             buildId, revision)
            context.write(filePath, {filterPathSD: {
                'values':   outarray,
                'buildId':  buildId,
                'revision': revision
            }})
        except TypeError:
            dict_locations = [p for p, t in enumerate(filterPathSD) if type(t) is dict]
            if dict_locations:
                field_names = ["buildDate", "reason", "appName", "OS",
                               "osVersion", "arch"]
                dict_field_names = [field_names[i] for i in dict_locations]
                msg = ("unable to hash the following `filterPathSD` fields: %s" %
                       (' '.join(dict_field_names)))
            else:
                msg = "TypeError when writing map output."
            if verbose:
                print >> sys.stderr, msg

    # by build date

    # Now read and output simple measures
    for sm_name, sm_value in payload.get('simpleMeasurements', {}).iteritems():
        # Handle cases where the value is a dictionary of simple measures
        if type(sm_value) == dict:
            for sub_name, sub_value in sm_value.iteritems():
                map_simplemeasure(channel, majorVersion, "by-build-date", filterPathBD,
                                  sm_name + "_" + sub_name, sub_value, context,
                                  buildId, revision)
        else:
            map_simplemeasure(channel, majorVersion, "by-build-date", filterPathBD,
                              sm_name, sm_value, context, buildId, revision)

    # by submission date
    if skip_by_submission_date:
        return

    # Now read and output simple measures
    for sm_name, sm_value in payload.get('simpleMeasurements', {}).iteritems():
        # Handle cases where the value is a dictionary of simple measures
        if type(sm_value) == dict:
            for sub_name, sub_value in sm_value.iteritems():
                map_simplemeasure(channel, majorVersion, "by-submission-date", filterPathSD,
                                  sm_name + "_" + sub_name, sub_value, context,
                                  buildId, revision)
        else:
            map_simplemeasure(channel, majorVersion, "by-submission-date", filterPathSD,
                              sm_name, sm_value, context, buildId, revision)


# Map a simple measure
def map_simplemeasure(channel, majorVersion, byDateType, filterPath, name, value,
                      context, buildId, revision):
    # Sanity check value
    if type(value) not in (int, long):
        if verbose:
            msg = ("%s is not a value type for simpleMeasurements \"%s\"" %
                   (type(value), name))
            print >> sys.stderr, msg
        return

    bucket = simple_measures_buckets[1]
    outarray = [0] * (len(bucket) + 6)
    for i in reversed(range(0, len(bucket))):
        if value >= bucket[i]:
            outarray[i] = 1
            break

    log_val = math.log(math.fabs(value) + 1)
    outarray[-6] = value                                # sum
    outarray[-5] = log_val                              # log_sum
    outarray[-4] = log_val * log_val                    # log_sum_squares
    outarray[-3] = -1                                   # sum_squares_lo
    outarray[-2] = -1                                   # sum_squares_hi
    outarray[-1] = 1                                    # count

    filePath = (channel, majorVersion, "SIMPLE_MEASURES_" + name.upper(), byDateType)

    # Output result array
    sanitize_and_put(context, filePath, filterPath, outarray, buildId, revision)

def commonCombine(values):
    out = {}
    for d in values:
        for filter_path, blob in d.iteritems():
            existing = out.get(filter_path, None)
            if existing is None:
                out[filter_path] = HistogramAggregator(**blob)
            else:
                existing.merge(**blob)
    for k, v in out.iteritems():
        out[k] = v.dump()
    return out


def combine(key, values, context):
    context.write(key, commonCombine(values))

def reduce(key, values, context):
    out = {}
    for filterPath, blob in commonCombine(values).iteritems():
        out["/".join(filterPath)] = blob
    context.write("/".join(key), json.dumps(out))

setupjob = telemetryutils.setupjob


