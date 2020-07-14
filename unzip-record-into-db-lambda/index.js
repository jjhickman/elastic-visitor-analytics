const aws = require('aws-sdk');
const mysql = require('mysql');
const zlib = require("zlib");

const s3 = new aws.S3({ apiVersion: '2006-03-01' });
const connection_params = {
    host: process.env.RDS_ENDPOINT,
    user: process.env.RDS_USERNAME,
    password: process.env.RDS_PASSWORD,
    database: process.env.RDS_DATABASE,
};

exports.handler = async (event, context) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };
    try {
        const { Body } = await s3.getObject(params).promise();
        console.log('Compressed Body:', Body);
        const connection = mysql.createConnection(connection_params);
        console.log(`Connected to ${connection_params.host} with database ${connection_params.database}`);
         // Calling gunzip method 
        zlib.gunzip(Body, (err, buffer) => {
            let queryRows = [];
            if (err) return console.log(err);

            let content = buffer.toString('utf8');
            console.log(`Deflated content: ${content}`); 

            let lines = content.split(/\r?\n/);
            console.log('Number of records: ', lines.length);
            if (lines.length == 0) return console.log(`File is empty, nothing to insert`);
            for (let i = 0; i < lines.length; i++) {
                queryRows.push(lines[i].split(','));
            }
            console.log('Query rows: ', queryRows);
            let query = 'INSERT INTO details_by_city_country (field1, field2, field3, fieldn) VALUES ?';
				connection.query(query, [queryRows], (error, response) => {
					console.log(error || response);
					return (error || response);
				});
        }); 
    } catch (err) {
        console.log(err);
        const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
        console.log(message);
        throw new Error(message);
    }
};