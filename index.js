const csv = require('csv-parser');
const fs = require('fs');

function readFromFile(file) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(file)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

const mapRow = (row) => {
    const { name, latitude, longitude } = row;
    return { name, latitude, longitude };
};

const unionAll = async () => {
    const mark = process.hrtime();
    const [barcelona1, barcelona2] = await Promise.all( [
        readFromFile('sources/Barcelona1.csv'),
        readFromFile('sources/Barcelona2.csv')
    ]);
    return {
        data: barcelona1.map(mapRow).concat(barcelona2.map(mapRow)),
        mark,
    };
};

const union = async () => {
    const mark = process.hrtime();
    const { data } = await unionAll();
    const set = new Set();
    data.forEach((row) => {
        set.add(JSON.stringify(row));
    });
    return {
        data: Array.from(set).map(row => JSON.parse(row)),
        mark,
    };
};

unionAll().then(({ data, mark }) => {
    const time = process.hrtime(mark);
    console.log('The result of UNION ALL is of size', data.length);
    console.log('The execution time of UNION ALL is %d seconds and %d nanoseconds', time[0], time[1]);
});

union().then(({ data, mark }) => {
    const time = process.hrtime(mark);
    console.log('The result of UNION is of size', data.length);
    console.log('The execution time of UNION ALL is %d seconds and %d nanoseconds', time[0], time[1]);
});