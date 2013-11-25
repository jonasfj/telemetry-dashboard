verbose = False
import math, sys
try:
    import simplejson as json
except ImportError:
    if verbose:
        print "Failed to load simplejson"
    import json
from datetime import datetime
import math, random



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
        self.values = list(values)
        self.buildId = buildId
        self.revision = revision

    def merge(self, values, buildId, revision):
        # If length of values don't match up, we have two different histograms
        if len(self.values) != len(values):
            # Choose the histogram with highest buildId
            if self.buildId < buildId:
                self.values = list(values)
                self.buildId = buildId
                self.revision = revision
        else:
            if self.buildId < buildId:
                self.revision = revision
                self.buildId = buildId
            for i in xrange(0, len(values) - 6):
                self.values[i] += values[i]
            # Entries [-6:-1] may have -1 indicating missing entry
            for i in xrange(len(values) - 6, len(values) - 1):
                # Missing entries are indicated with -1, we shouldn't add these up
                if self.values[i] == -1 and values[i] == -1:
                    continue
                self.values[i] += values[i]
            # Last entry cannot be negative
            self.values[-1] += values[-1]

    def dump(self):
        replace_nan_inf(self.values)
        return {
            'revision':     self.revision,
            'buildId':      self.buildId,
            'values':       self.values
        }

def replace_nan_inf(values):
    """ Replace NaN and Inf with null and float.max respectively """
    for i in xrange(0, len(values)):
        val = values[i]
        if math.isinf(val):
            if val < 0:
                values[i] = - sys.float_info.max
            else:
                values[i] = sys.float_info.max
        elif math.isnan(val):
            # this isn't good... but we can't handle all possible corner cases
            # NaN shouldn't be possible... besides it's not known to happen
            values[i] = None

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
puts = 0

def sanitize_and_put(cache, filePath, filterPath, values, buildId, revision):
    global puts
    puts += 1

    key = "/".join(filePath)
    fcahce = cache.get(key, None)
    if fcahce == None:
        fcahce = {}
        cache[key] = fcahce

    agg = fcahce.get(filterPath, None)
    if agg == None:
        agg = HistogramAggregator(values, buildId, revision)
        fcahce[filterPath] = agg
    else:
        agg.merge(values, buildId, revision)

def log(msg):
    if verbose:
        print >> sys.stderr, msg

SPECS = "histogram_specs.json"
with open(SPECS, 'r') as f:
    histogram_specs = json.loads(f.read())

def map(submissionDate, line, cache):
    global histogram_specs


    payload = json.loads(line)

    try:
        i = payload['info']
        channel = i.get('appUpdateChannel', "pre-11")
        OS = i['OS']
        appName = i['appName']
        reason = i['reason']
        osVersion = str(i['version'])
        #only care about major versions
        majorVersion = i['appVersion'].split('.')[0]
        arch = i['arch']
        buildId = i['appBuildID']
        buildDate = buildId[:8]
        revision = i.get('revision', 'http://hg.mozilla.org/mozilla-central/rev/7b014f0f3b03')
    except (KeyError, IndexError, UnicodeEncodeError):
        return

    # TODO: histogram_specs should specify the list of versions/channels we
    #       care about
    if not channel in ['pre-11', 'release', 'aurora', 'nightly', 'beta', 'nightly-ux']:
        return

    # todo combine OS + osVersion + santize on crazy platforms like linux to
    #      reduce pointless choices
    if OS == "Linux":
        osVersion = osVersion[:3]

    try:
        filterPathBD = (buildDate, reason, appName, OS, osVersion, arch)
        filterPathBD = "/".join(filterPathBD)
        filterPathSD = (submissionDate, reason, appName, OS, osVersion, arch)
        filterPathSD = "/".join(filterPathSD)
    except:
        return

    # Sanitize channel and majorVersion
    for val in (channel, majorVersion):
        if not isinstance(val, basestring) and type(val) in (int, float, long):
            return

    bdate = datetime.strptime(buildDate, "%Y%m%d")
    sdate = datetime.strptime(submissionDate, "%Y%m%d")
    skip_by_submission_date = True
    if (sdate - bdate).days < 60:
        skip_by_submission_date = False

    ######### dimensions and other fields are loaded and sanitized

    channelmajorVersion = channel + "/" + majorVersion

    for h_name, h_values in payload.get('histograms', {}).iteritems():
        bucket2index = histogram_specs.get(h_name, None)
        if bucket2index is None and h_name.startswith('STARTUP_'):
            bucket2index = histogram_specs.get(h_name[8:], None)
        if bucket2index is None:
            continue
        else:
            bucket2index = bucket2index[0]

        # most buckets contain 0s, so preallocation is a significant win
        outarray = [0] * (len(bucket2index) + 6)

        error = False
        try:
            values = h_values['values']
            for bucket, value in values.iteritems():
                index = bucket2index.get(bucket, None)
                if index is None:
                    error = True
                    break
                value = value or 0
                if type(value) not in (int, long, float):
                    error = True
                    break
                outarray[index] = value
            if error:
                continue
        except:
            continue

        outarray[-6] = h_values.get('sum', -1)              # sum
        outarray[-5] = h_values.get('log_sum', -1)          # log_sum
        outarray[-4] = h_values.get('log_sum_squares', -1)  # log_sum_squares
        outarray[-3] = h_values.get('sum_squares_lo', -1)   # sum_squares_lo
        outarray[-2] = h_values.get('sum_squares_hi', -1)   # sum_squares_hi
        for i in xrange(-6, -1):
            if outarray[i] is None:
                outarray[i] = 0
        outarray[-1] = 1                                    # count

        # by build date

        filePath = (channelmajorVersion, h_name, "by-build-date")
        sanitize_and_put(cache, filePath, filterPathBD, outarray, buildId, revision)

        # by submission date
        if skip_by_submission_date:
            continue

        filePath = (channelmajorVersion, h_name, "by-submission-date")
        sanitize_and_put(cache, filePath, filterPathSD, outarray, buildId, revision)

    # by build date

    # Now read and output simple measures
    for sm_name, sm_value in payload.get('simpleMeasurements', {}).iteritems():
        # Handle cases where the value is a dictionary of simple measures
        if type(sm_value) == dict:
            for sub_name, sub_value in sm_value.iteritems():
                map_simplemeasure(channelmajorVersion, "by-build-date", filterPathBD,
                                  sm_name + "_" + sub_name, sub_value, cache,
                                  buildId, revision)
        else:
            map_simplemeasure(channelmajorVersion, "by-build-date", filterPathBD,
                              sm_name, sm_value, cache, buildId, revision)

        # by submission date
        if skip_by_submission_date:
            continue

        # Handle cases where the value is a dictionary of simple measures
        if type(sm_value) == dict:
            for sub_name, sub_value in sm_value.iteritems():
                map_simplemeasure(channelmajorVersion, "by-submission-date", filterPathSD,
                                  sm_name + "_" + sub_name, sub_value, cache,
                                  buildId, revision)
        else:
            map_simplemeasure(channelmajorVersion, "by-submission-date", filterPathSD,
                              sm_name, sm_value, cache, buildId, revision)


# Map a simple measure
def map_simplemeasure(channelmajorVersion, byDateType, filterPath, name, value,
                      cache, buildId, revision):
    # Sanity check value
    if type(value) not in (int, long):
        return

    return
    bucket = simple_measures_buckets[1]
    outarray = [0] * (len(bucket) + 6)
    for i in reversed(xrange(0, len(bucket))):
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

    filePath = (channelmajorVersion, "SIMPLE_MEASURES_" + name.upper(), byDateType)

    # Output result array
    sanitize_and_put(cache, filePath, filterPath, outarray, buildId, revision)



############ RUN
from subprocess import Popen, PIPE

fail = 0
skips = 0
rows = 0
cache = {}
for line in sys.stdin:
    meta, data = line.split('\t')
    old_puts = puts
    rows += 1
    try:
        map(meta[:8], data, cache)
    except:
        fail += 1
    if old_puts == puts:
        skips += 1
    if rows % 100000 == 0:
        print " - (rows: %i, puts: %i, skips: %i, fail: %i)" % (rows, puts, skips, fail)

with open(sys.argv[1], 'w') as f:
    for filePath, blob in cache.iteritems():
        for filterPath, agg in blob.iteritems():
            blob[filterPath] = agg.dump()
        f.write(filePath + "\t")
        f.write(json.dumps(blob))
        f.write('\n')

print "Finished with (rows: %i, puts: %i, skips: %i, fail: %i)" % (rows, puts, skips, fail)
