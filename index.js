const fs = require('fs');
const archiver = require('archiver');
const { createObjectCsvStringifier } = require('csv-writer');

// Configura el CSV Stringifier (para escribir directamente en un stream)
const csvStringifier = createObjectCsvStringifier({
  header: [
    { id: 'field1', title: 'Field 1' },
    { id: 'field2', title: 'Field 2' },
    { id: 'field3', title: 'Field 3' },
    { id: 'field4', title: 'Field 4' },
    { id: 'field5', title: 'Field 5' },
    { id: 'field6', title: 'Field 6' },
    { id: 'field7', title: 'Field 7' },
    { id: 'field8', title: 'Field 8' },
    { id: 'field9', title: 'Field 9' },
    { id: 'field10', title: 'Field 10' },
    { id: 'field11', title: 'Field 11' },
    { id: 'field12', title: 'Field 12' },
    { id: 'field13', title: 'Field 13' },
    { id: 'field14', title: 'Field 14' },
    { id: 'field15', title: 'Field 15' },
    { id: 'field16', title: 'Field 16' },
    { id: 'field17', title: 'Field 17' },
    { id: 'field18', title: 'Field 18' },
    { id: 'field19', title: 'Field 19' },
    { id: 'field20', title: 'Field 20' },
    { id: 'field21', title: 'Field 21' },
    { id: 'field22', title: 'Field 22' },
    { id: 'field23', title: 'Field 23' },
    { id: 'field24', title: 'Field 24' },
    { id: 'field25', title: 'Field 25' },
    { id: 'field26', title: 'Field 26' },
    { id: 'field27', title: 'Field 27' },
    { id: 'field28', title: 'Field 28' },
    { id: 'field29', title: 'Field 29' },
    { id: 'field30', title: 'Field 30' },
    { id: 'field31', title: 'Field 31' },
    { id: 'field32', title: 'Field 32' },
    { id: 'field33', title: 'Field 33' },
    { id: 'field34', title: 'Field 34' },
    { id: 'field35', title: 'Field 35' },
    { id: 'field36', title: 'Field 36' },
    { id: 'field37', title: 'Field 37' },
    { id: 'field38', title: 'Field 38' },
    { id: 'field39', title: 'Field 39' },
    { id: 'field40', title: 'Field 40' }
  ]
});

// Función para crear un flujo CSV
function writeCSVStream(stream, numRows) {
  const chunkSize = 10000;  // Procesar en fragmentos de 10,000 filas
  return new Promise((resolve, reject) => {
    let currentRow = 0;

    function writeNextChunk() {
      while (currentRow < numRows) {
        const chunk = [];

        // Genera el chunk de datos
        for (let i = 0; i < chunkSize && currentRow < numRows; i++, currentRow++) {
          chunk.push({
            field1: `value1_${i}====`,
            field2: `value2_${i}====`,
            field3: `value3_${i}====`,
            field4: `value4_${i}====`,
            field5: `value5_${i}====`,
            field6: `value6_${i}====`,
            field7: `value7_${i}====`,
            field8: `value8_${i}====`,
            field9: `value9_${i}====`,
            field10: `value10_${i}====`,
            field11: `value11_${i}====`,
            field12: `value12_${i}====`,
            field13: `value13_${i}====`,
            field14: `value14_${i}====`,
            field15: `value15_${i}====`,
            field16: `value16_${i}====`,
            field17: `value17_${i}====`,
            field18: `value18_${i}====`,
            field19: `value19_${i}====`,
            field20: `value20_${i}====`,
            field21: `value21_${i}====`,
            field22: `value22_${i}====`,
            field23: `value23_${i}====`,
            field24: `value24_${i}====`,
            field25: `value25_${i}====`,
            field26: `value26_${i}====`,
            field27: `value27_${i}====`,
            field28: `value28_${i}====`,
            field29: `value29_${i}====`,
            field30: `value30_${i}====`,
            field31: `value31_${i}====`,
            field32: `value32_${i}====`,
            field33: `value33_${i}====`,
            field34: `value34_${i}====`,
            field35: `value35_${i}====`,
            field36: `value36_${i}====`,
            field37: `value37_${i}====`,
            field38: `value38_${i}====`,
            field39: `value39_${i}====`,
            field40: `value40_${i}==== `
          });
        }

        // Escribe el chunk en el flujo
        const csvContent = csvStringifier.stringifyRecords(chunk);

        if (!stream.write(csvContent)) {
          // Si el stream está ocupado, espera a que drene
          stream.once('drain', writeNextChunk);
          return;
        }
      }

      // Cierra el stream cuando se complete
      if (currentRow >= numRows) {
        stream.end();
        resolve();
      }
    }

    writeNextChunk();
  });
}

// Función para comprimir el CSV a ZIP usando flujos
function createZipStream(outputZipPath, inputCsvStreamPath) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(outputZipPath);
    const archive = archiver('zip', {
      zlib: { level: 9 }  // Nivel máximo de compresión
    });

    output.on('close', () => {
      console.log(`ZIP file has been created. Total size: ${archive.pointer()} bytes`);
      resolve();
    });

    archive.on('error', (err) => {
      reject(err);
    });

    archive.pipe(output);

    // Añade el archivo CSV al ZIP
    archive.file(inputCsvStreamPath, { name: 'large_file.csv' });

    archive.finalize();
  });
}

(async () => {
  const csvFilePath = './output/large_file.csv';
  const zipFilePath = './output/large_file.zip';
  
  const csvStream = fs.createWriteStream(csvFilePath);

  // Guardar la hora de inicio
  const startTime = new Date();
  console.log(`Process started at: ${startTime}`);

  try {
    console.log('Starting CSV generation...');
    await writeCSVStream(csvStream, 9000000);  // Generar 3 millones de filas
    console.log('CSV generated successfully.');

    console.log('Creating ZIP file...');
    await createZipStream(zipFilePath, csvFilePath);
    console.log('ZIP file created successfully.');
  } catch (error) {
    console.error('Error during the process:', error);
  }

  // Guardar la hora de fin
  const endTime = new Date();
  console.log(`Process finished at: ${endTime}`);

  // Calcular la duración
  const duration = (endTime - startTime) / 1000;  // en segundos
  console.log(`Total time taken: ${duration} seconds`);
})();
