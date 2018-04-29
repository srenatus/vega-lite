import {isBoolean, isNumber} from 'vega-util';
import {isAggregateOp} from '../aggregate';
import {binToString} from '../bin';
import {Channel} from '../channel';
import {Config} from '../config';
import {reduce} from '../encoding';
import {GenericMarkDef, isMarkDef, Mark, MarkConfig, MarkDef, MarkProperties} from '../mark';
import {AggregatedFieldDef, BinTransform, CalculateTransform, TimeUnitTransform} from '../transform';
import {Flag, keys} from '../util';
import {Encoding, forEach} from './../encoding';
import {Field, FieldDef, isContinuous, isFieldDef, PositionFieldDef, vgField} from './../fielddef';
import * as log from './../log';
import {GenericUnitSpec, NormalizedLayerSpec, NormalizedUnitSpec} from './../spec';
import {Orient} from './../vega.schema';
import {partLayerMixins} from './common';

export const ERRORBAR: 'errorbar' = 'errorbar';
export type ErrorBar = typeof ERRORBAR;

export type ErrorBarExtent = 'ci' | 'iqr' | 'stderr' | 'stdev';
export type ErrorBarCenter = 'mean' | 'median';
export type ErrorBarSinglePointPart = 'bar' | 'line' | 'point' | 'ticks';

const ERROR_BAR_SINGLE_POINT_PART_INDEX: Flag<ErrorBarSinglePointPart> = {
  bar: 1,
  line: 1,
  point: 1,
  ticks: 1
};

export function isErrorBarSinglePointPart(a: string): a is ErrorBarSinglePointPart {
  return !!ERROR_BAR_SINGLE_POINT_PART_INDEX[a];
}

export type ErrorBarPart = ErrorBarSinglePointPart | 'whisker';

const markNameOfErrorBarPart: { [key: string]: Mark; }= {
  bar: 'bar',
  line: 'line',
  point: 'point',
  ticks: 'tick',
  whisker: 'rule'
};

export function getMarkNameOfErrorBarPart(key: ErrorBarPart): Mark {
  if (!markNameOfErrorBarPart[key]) {
    log.warn(`wrong ErrorBarPart is used (${key} is entered)`);
  }
  return (markNameOfErrorBarPart[key]) ? markNameOfErrorBarPart[key] : 'point';
}

const ERRORBAR_PART_INDEX: Flag<ErrorBarPart> = {
  bar: 1,
  line: 1,
  point: 1,
  ticks: 1,
  whisker: 1
};

export const ERRORBAR_PARTS = keys(ERRORBAR_PART_INDEX);

// TODO: Currently can't use `PartsMixins<ErrorBarPart>`
// as the schema generator will fail
export type ErrorBarPartsMixins = {
  [part in ErrorBarPart]?: boolean | MarkConfig
};

export interface ErrorBarConfig extends ErrorBarPartsMixins {

  /**
   * The extent of the whiskers. Available options include:
   * - `"ci": Extend the whiskers to the confidence interval of the mean.
   * - `"stderr": The size of whiskers are set to the value of standard error, extending from the center.
   * - `"stdev": The size of whiskers are set to the value of standard deviation, extending from the center.
   * - `"iqr": Extend the whiskers to the q1 and q3.
   *
   * __Default value:__ `"stderr"`.
   */
  extent?: ErrorBarExtent;

  /**
   * The center of the errorbar. Available options include:
   * - `"mean": the mean of the data points.
   * - `"median": the median of the data points.
   *
   * __Default value:__ `"mean"`.
   */

  center?: ErrorBarCenter;
}

export interface ErrorBarDef extends GenericMarkDef<ErrorBar>, ErrorBarConfig {
  /**
   * Orientation of the error bar.  This is normally automatically determined, but can be specified when the orientation is ambiguous and cannot be automatically determined.
   */
  orient?: Orient;
}

export interface ErrorBarConfigMixins {
  /**
   * ErrorBar Config
   */
  errorbar?: ErrorBarConfig;
}


const supportedChannels: Channel[] = ['x', 'y', 'color', 'detail', 'opacity', 'size'];
export function filterUnsupportedChannels(spec: GenericUnitSpec<Encoding<string>, ErrorBar | ErrorBarDef>): GenericUnitSpec<Encoding<string>, ErrorBar | ErrorBarDef> {
  return {
    ...spec,
    encoding: reduce(spec.encoding, (newEncoding, fieldDef, channel) => {
      if (supportedChannels.indexOf(channel) > -1) {
        newEncoding[channel] = fieldDef;
      } else {
        log.warn(log.message.incompatibleChannel(channel, ERRORBAR));
      }
      return newEncoding;
    }, {}),
  };
}

export function normalizeErrorBar(spec: GenericUnitSpec<Encoding<string>, ErrorBar | ErrorBarDef>, config: Config): NormalizedLayerSpec {
  spec = filterUnsupportedChannels(spec);
  // TODO: use selection
  const {mark, encoding, selection, projection: _p, ...outerSpec} = spec;
  const markDef: ErrorBarDef = isMarkDef(mark) ? mark : {type: mark};

  const center: ErrorBarCenter = markDef.center || config.errorbar.center;
  let extent: ErrorBarExtent = markDef.extent || config.errorbar.extent;

  if (center === 'median') {
    extent = markDef.extent || 'iqr';
  }

  // add warning check in the test in this format.
  // it('warn if size is data driven and autosize is fit', log.wrap((localLogger) => {
  //   const spec = compile({
  //     "data": {"values": [{"a": "A","b": 28}]},
  //     "mark": "bar",
  //     "autosize": "fit",
  //     "encoding": {
  //       "x": {"field": "a", "type": "ordinal"},
  //       "y": {"field": "b", "type": "quantitative"}
  //     }
  //   }).spec;
  //   assert.equal(localLogger.warns[0], log.message.CANNOT_FIX_RANGE_STEP_WITH_FIT);
  //   assert.equal(spec.width, 200);
  //   assert.equal(spec.height, 200);
  // }));
  if ((center === 'median') !== (extent === 'iqr')) {
    log.warn(`${center} is not usually used with ${extent} for error bar.`);
  }

  const orient: Orient = errorBarOrient(spec);
  const {transform, continuousAxisChannelDef, continuousAxis, groupby, encodingWithoutContinuousAxis} = errorBarParams(spec, orient, center, extent);

  const {color, size, ...encodingWithoutSizeColorAndContinuousAxis} = encodingWithoutContinuousAxis;

  const {scale, axis} = continuousAxisChannelDef;

  const errorBarSinglePointPartMarksSpec = function(partName: ErrorBarPart, fieldPrefix: string = center) {
    const markProperties: boolean | object = config.errorbar[partName];
    const defaultMarkDef: Mark | MarkDef = (isBoolean(markProperties)) ? getMarkNameOfErrorBarPart(partName) : {
      type: getMarkNameOfErrorBarPart(partName),
      ...markProperties
    };

    const isSinglePoint: boolean = isErrorBarSinglePointPart(partName);

    return partLayerMixins<ErrorBarPartsMixins>(
      markDef, partName, config.errorbar,
      {
        mark: defaultMarkDef,
        encoding: {
          [continuousAxis]: {
            field: (isSinglePoint ? fieldPrefix : 'lower_whisker') + '_' + continuousAxisChannelDef.field,
            type: continuousAxisChannelDef.type,
            ...(scale ? {scale} : {}),
            ...(axis ? {axis} : {})
          },
          ...(isSinglePoint ? {} : {
            [continuousAxis + '2']: {
              field: 'upper_whisker_' + continuousAxisChannelDef.field,
              type: continuousAxisChannelDef.type
            }
          }),
          ...encodingWithoutSizeColorAndContinuousAxis
        }
      },
      !!markProperties
    );
  };

  return {
    ...outerSpec,
    transform,
    layer: [
      ...errorBarSinglePointPartMarksSpec('bar'),
      ...errorBarSinglePointPartMarksSpec('line'),
      ...errorBarSinglePointPartMarksSpec('ticks', 'lower_whisker'),
      ...errorBarSinglePointPartMarksSpec('ticks', 'upper_whisker'),
      ...errorBarSinglePointPartMarksSpec('whisker'),
      ...errorBarSinglePointPartMarksSpec('point')
    ]
  };
}

function errorBarOrient(spec: GenericUnitSpec<Encoding<Field>, ErrorBar | ErrorBarDef>): Orient {
  const {mark: mark, encoding: encoding, projection: _p, ..._outerSpec} = spec;

  if (isFieldDef(encoding.x) && isContinuous(encoding.x)) {
    // x is continuous
    if (isFieldDef(encoding.y) && isContinuous(encoding.y)) {
      // both x and y are continuous
      if (encoding.x.aggregate === undefined && encoding.y.aggregate === ERRORBAR) {
        return 'vertical';
      } else if (encoding.y.aggregate === undefined && encoding.x.aggregate === ERRORBAR) {
        return 'horizontal';
      } else if (encoding.x.aggregate === ERRORBAR && encoding.y.aggregate === ERRORBAR) {
        throw new Error('Both x and y cannot have aggregate');
      } else {
        if (isMarkDef(mark) && mark.orient) {
          return mark.orient;
        }

        // default orientation = vertical
        return 'vertical';
      }
    }

    // x is continuous but y is not
    return 'horizontal';
  } else if (isFieldDef(encoding.y) && isContinuous(encoding.y)) {
    // y is continuous but x is not
    return 'vertical';
  } else {
    // Neither x nor y is continuous.
    throw new Error('Need a valid continuous axis for errorbars');
  }
}


function errorBarContinousAxis(spec: GenericUnitSpec<Encoding<string>, ErrorBar | ErrorBarDef>, orient: Orient) {
  const {mark: mark, encoding: encoding, projection: _p, ..._outerSpec} = spec;

  let continuousAxisChannelDef: PositionFieldDef<string>;
  let continuousAxis: 'x' | 'y';

  if (orient === 'vertical') {
    continuousAxis = 'y';
    continuousAxisChannelDef = encoding.y as FieldDef<string>; // Safe to cast because if y is not continuous fielddef, the orient would not be vertical.
  } else {
    continuousAxis = 'x';
    continuousAxisChannelDef = encoding.x as FieldDef<string>; // Safe to cast because if x is not continuous fielddef, the orient would not be horizontal.
  }

  if (continuousAxisChannelDef && continuousAxisChannelDef.aggregate) {
    const {aggregate, ...continuousAxisWithoutAggregate} = continuousAxisChannelDef;
    if (aggregate !== ERRORBAR) {
      log.warn(`Continuous axis should not have customized aggregation function ${aggregate}`);
    }
    continuousAxisChannelDef = continuousAxisWithoutAggregate;
  }

  return {
    continuousAxisChannelDef,
    continuousAxis
  };
}

function isExtentCI(extent: ErrorBarExtent): extent is 'ci' {
  return extent === 'ci';
}

function isExtentIqr(extent: ErrorBarExtent): extent is 'iqr' {
  return extent === 'iqr';
}

function errorBarSpecialExtents(extent: 'ci' | 'iqr', fieldName: string): AggregatedFieldDef[] {
  return [
    {
      op: (extent === 'ci') ? 'ci0' : 'q1',
      field: fieldName,
      as: 'lower_whisker_' + fieldName
    },
    {
      op: (extent === 'ci') ? 'ci1' : 'q3',
      field: fieldName,
      as: 'upper_whisker_' + fieldName
    }
  ];
}

function errorBarParams(spec: GenericUnitSpec<Encoding<string>, ErrorBar | ErrorBarDef>, orient: Orient, center: ErrorBarCenter, extent: ErrorBarExtent) {

  const {continuousAxisChannelDef, continuousAxis} = errorBarContinousAxis(spec, orient);
  const encoding = spec.encoding;

  const continuousFieldName: string = continuousAxisChannelDef.field;

  let aggregate: AggregatedFieldDef[] = [
    {
      op: center,
      field: continuousFieldName,
      as: center + '_' + continuousFieldName
    }
  ];

  let postAggregateCalculates: CalculateTransform[] = [];

  if (isExtentCI(extent)) {
    aggregate = aggregate.concat(errorBarSpecialExtents('ci', continuousFieldName));
  } else if (isExtentIqr(extent)) {
    aggregate = aggregate.concat(errorBarSpecialExtents('iqr', continuousFieldName));
  } else {
    aggregate.push({
      op: extent,
      field: continuousFieldName,
      as: 'extent_' + continuousFieldName
    });

    postAggregateCalculates = [{
        calculate: `datum.${center}_${continuousFieldName} + datum.extent_${continuousFieldName}`,
        as: 'upper_whisker_' + continuousFieldName
      },
      {
        calculate: `datum.${center}_${continuousFieldName} - datum.extent_${continuousFieldName}`,
        as: 'lower_whisker_' + continuousFieldName
    }];
  }


  const groupby: string[] = [];
  const bins: BinTransform[] = [];
  const timeUnits: TimeUnitTransform[] = [];

  const encodingWithoutContinuousAxis: Encoding<string> = {};
  forEach(encoding, (channelDef, channel) => {
    if (channel === continuousAxis) {
      // Skip continuous axis as we already handle it separately
      return;
    }
    if (isFieldDef(channelDef)) {
      if (channelDef.aggregate && isAggregateOp(channelDef.aggregate)) {
        aggregate.push({
          op: channelDef.aggregate,
          field: channelDef.field,
          as: vgField(channelDef)
        });
      } else if (channelDef.aggregate === undefined) {
        const transformedField = vgField(channelDef);

        // Add bin or timeUnit transform if applicable
        const bin = channelDef.bin;
        if (bin) {
          const {field} = channelDef;
          bins.push({bin, field, as: transformedField});
        } else if (channelDef.timeUnit) {
          const {timeUnit, field} = channelDef;
          timeUnits.push({timeUnit, field, as: transformedField});
        }

        groupby.push(transformedField);
      }
      // now the field should refer to post-transformed field instead
      encodingWithoutContinuousAxis[channel] = {
        field: vgField(channelDef),
        type: channelDef.type
      };
    } else {
      // For value def, just copy
      encodingWithoutContinuousAxis[channel] = encoding[channel];
    }
  });

  return {
    transform: [].concat(
      bins,
      timeUnits,
      [{aggregate, groupby}],
      postAggregateCalculates
    ),
    groupby,
    continuousAxisChannelDef,
    continuousAxis,
    encodingWithoutContinuousAxis
  };
}
