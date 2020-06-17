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

/* The following is an example of 'records':
 * [
 *  [ 'ALL', '2020-02-15', '-1', '4', '7', '1', '4', '0' ].
 *  [ 'ALL', '2020-02-16', '-9', '-6', '-35', '-10', '-2', '2' ],
 *  ...
 * ]
 */
const writePrefectureRecords = (
  records: string[][],
  prefecture: string,
  publishedOn: string,
  promises: Promise<void>[]
): void => {
  const recordsByDate: RecordsByDate = {};

  for (const r of records) {
    // The value of r[1] woule be like '2020-02-15'
    recordsByDate[r[1]] = {
      retailAndRecreation: Number.parseInt(r[2]),
      groceryAndPharmacy: Number.parseInt(r[3]),
      parks: Number.parseInt(r[4]),
      transitStations: Number.parseInt(r[5]),
      workplaces: Number.parseInt(r[6]),
      residential: Number.parseInt(r[7]),
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
  records: string[][],
  publishedOn: string,
  promises: Promise<void>[]
): Promise<void> => {
  const prefectures = new Set(records.map((r) => r[0]));
  for (const p of prefectures) {
    const recordsOfCurrentPrefecture = records.filter((r) => r[0] === p);
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
      from_line: 2,
      on_record: (record: string[]) => { // eslint-disable-line prettier/prettier
        record.shift(); // Skip country_region_code
        record.shift(); // Skip country_region
        let area = record.shift(); // Get sub_region_1
        if (area === '') area = 'ALL';
        record.shift(); // Skip sub_region_2

        return [area, ...record]; // e.g. [ 'ALL', '2020-02-15', '-1', '4', '7', '1', '4', '0' ]
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
