const { createClient } = require('@supabase/supabase-js');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

// Supabase credentials from environment variables
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Create Supabase client
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  }
});

// Directory containing GeoJSON files
const geoJsonDirectory = './geo-jsons';

async function uploadGeoJSON() {
  try {
    // Read all files in the directory
    const files = fs.readdirSync(geoJsonDirectory);

    // Filter for GeoJSON files
    const geoJsonFiles = files.filter(file => file.endsWith('.geojson'));
    
    console.log(`Found ${geoJsonFiles.length} GeoJSON files to process`);

    // Iterate over each GeoJSON file
    for (const file of geoJsonFiles) {
      const filePath = path.join(geoJsonDirectory, file);
      console.log(`Processing file: ${file}`);
      
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const geojsonData = JSON.parse(fileContent);

      console.log(`File contains ${geojsonData.features.length} features`);
      
      // Track progress
      let successCount = 0;
      let errorCount = 0;
      
      // Iterate over each feature in the GeoJSON file
      for (const feature of geojsonData.features) {
        const { properties, geometry } = feature;
        
        try {
          // Step 1: Insert the record with properties but null geometry
          const { data, error } = await supabase
            .from('spatial_data')
            .insert({
              properties: properties,
              geometry: null  // Will be filled in the next step
            })
            .select('id')
            .single();

          if (error) {
            console.error('Error inserting data:', error);
            errorCount++;
            continue;
          }

          // Step 2: Update the geometry using raw SQL with ST_GeomFromGeoJSON
          const geometryString = JSON.stringify(geometry);
          const { error: updateError } = await supabase
            .rpc('pgis_update_geometry', { 
              row_id: data.id, 
              geom_json: geometryString 
            });

          if (updateError) {
            console.error('Error updating geometry:', updateError);
            errorCount++;
          } else {
            successCount++;
            // Log progress every 10 features
            if (successCount % 10 === 0) {
              console.log(`Processed ${successCount} features successfully`);
            }
          }
        } catch (featureError) {
          console.error('Error processing feature:', featureError);
          errorCount++;
        }
      }

      console.log(`Completed file ${file}: ${successCount} successful, ${errorCount} errors`);
    }
    
    console.log('All GeoJSON files processed');
    
  } catch (error) {
    console.error('Error reading directory or files:', error);
  }
}

// First, create the necessary function in your database
async function setupDatabase() {
  console.log('Setting up database function...');
  
  const createFunctionQuery = `
  CREATE OR REPLACE FUNCTION pgis_update_geometry(row_id INT, geom_json TEXT)
  RETURNS VOID AS $$
  BEGIN
    UPDATE spatial_data
    SET geometry = ST_GeomFromGeoJSON(geom_json)
    WHERE id = row_id;
  END;
  $$ LANGUAGE plpgsql SECURITY DEFINER;
  `;
  
  const { error } = await supabase.rpc('exec_sql', { sql: createFunctionQuery });
  
  if (error) {
    console.error('Error creating function:', error);
    console.log('You may need to manually create the function using the SQL editor in Supabase');
  } else {
    console.log('Database function created successfully');
  }
}

// Run the script
(async () => {
  try {
    await setupDatabase();
    await uploadGeoJSON();
  } catch (error) {
    console.error('Script execution failed:', error);
  }
})();