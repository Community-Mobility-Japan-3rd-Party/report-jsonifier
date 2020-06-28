///<reference path="./promise.d.ts" />

import parse from 'csv-parse';
import { promises as fsp } from 'fs';

function parsePromisified(
  input: Buffer | string,
  options?: parse.Options
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): Promise<any> {
  return new Promise((resolve, reject) => {
    parse(input, options, (err, output) => {
      if (err) reject(err);
      resolve(output);
    });
  });
}

/* Defined the only properties we just need */
interface RawRecord {
  sub_region_1: string;
  date: string;
  retail_and_recreation_percent_change_from_baseline: number;
  grocery_and_pharmacy_percent_change_from_baseline: number;
  parks_percent_change_from_baseline: number;
  transit_stations_percent_change_from_baseline: number;
  workplaces_percent_change_from_baseline: number;
  residential_percent_change_from_baseline: number;
}

type DailyChangeRecord = {
  retailAndRecreation: number;
  groceryAndPharmacy: number;
  parks: number;
  transitStations: number;
  workplaces: number;
  residential: number;
};

type RecordsByDate = {
  [date: string]: DailyChangeRecord;
};

const writePrefectureRecords = (
  records: RawRecord[],
  prefecture: string,
  publishedOn: string,
  promises: Promise<void>[]
): void => {
  const recordsByDate: RecordsByDate = {};

  for (const r of records) {
    recordsByDate[r.date] = {
      retailAndRecreation: r.retail_and_recreation_percent_change_from_baseline,
      groceryAndPharmacy: r.grocery_and_pharmacy_percent_change_from_baseline,
      parks: r.parks_percent_change_from_baseline,
      transitStations: r.transit_stations_percent_change_from_baseline,
      workplaces: r.workplaces_percent_change_from_baseline,
      residential: r.residential_percent_change_from_baseline,
    };
  }

  const output: { [prefecture: string]: RecordsByDate } = {};
  output[prefecture] = recordsByDate;

  const fileOutput = `./dest/${publishedOn}/${prefecture}.json`;

  console.log(` -- Writing ${fileOutput}`);
  promises.push(
    fsp
      .mkdir(`./dest/${publishedOn}`, { recursive: true })
      .catch((e) => {
        // Ignore the errors of 'the directory already exsists'
        if (e.code !== 'EEXIST') throw e;
      })
      .then(() => fsp.writeFile(fileOutput, JSON.stringify(output, null, 2)))
  );
};

const processRecords = async (
  records: RawRecord[],
  publishedOn: string,
  promises: Promise<void>[]
): Promise<void> => {
  const prefectures = new Set(records.map((r) => r.sub_region_1));
  for (const p of prefectures) {
    const recordsOfCurrentPrefecture = records.filter(
      (r) => r.sub_region_1 === p
    );
    writePrefectureRecords(
      recordsOfCurrentPrefecture,
      p,
      publishedOn,
      promises
    );
  }
};

(async (): Promise<void> => {
  const promisesWriting: Promise<void>[] = [];
  const filesOrigin = await fsp.readdir('./origin');

  for (const f of filesOrigin) {
    const exec = /Mobility_Report_(\d{8}).csv/.exec(f);
    if (!exec) continue;

    const publishedOn = exec[1];
    console.log(`Reading ${f}`);
    const content = await fsp.readFile(`./origin/${f}`);
    const parsed = await parsePromisified(content, {
      from_line: 1,
      columns: true,
      on_record: (record: RawRecord) => { // eslint-disable-line prettier/prettier
        if (record.sub_region_1 === '') record.sub_region_1 = 'ALL';
        return record;
      },
    });
    processRecords(parsed, publishedOn, promisesWriting);
  }

  const results = await Promise.allSettled(promisesWriting);

  const fulfilled = results.filter((r) => r.status === 'fulfilled');
  const rejected = results.filter((r) => r.status === 'rejected');

  console.log(
    `
----------------------------------------
Success: ${fulfilled.length} items. Failed: ${rejected.length} items`
  );

  if (rejected.length > 0) {
    console.error('Error logs are shown as follows:');
    for (const r of rejected) {
      console.error(`  ${r.reason}`);
    }
  }

  process.exit(0);
})();
