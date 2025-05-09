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
      
      // Generate table name from file name (remove .geojson extension and sanitize)
      let tableName = file.replace('.geojson', '').toLowerCase();
      // Replace any non-alphanumeric characters with underscores
      tableName = tableName.replace(/[^a-z0-9]/g, '_');
      
      console.log(`Creating table: ${tableName}`);
      
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const geojsonData = JSON.parse(fileContent);

      console.log(`File contains ${geojsonData.features.length} features`);
      
      // Create a new table for this file
      await createTableForGeojson(tableName, geojsonData);
      
      // Track progress
      let successCount = 0;
      let errorCount = 0;
      
      // Iterate over each feature in the GeoJSON file
      for (const feature of geojsonData.features) {
        const { properties, geometry } = feature;
        
        try {
          // Step 1: Insert the record with properties but null geometry
          const { data, error } = await supabase
            .from(tableName)
            .insert({
              properties: properties,
              geometry: null  // Will be filled in the next step
            })
            .select('id')
            .single();

          if (error) {
            console.error(`Error inserting data into ${tableName}:`, error);
            errorCount++;
            continue;
          }

          // Step 2: Update the geometry using raw SQL with ST_GeomFromGeoJSON
          const geometryString = JSON.stringify(geometry);
          const { error: updateError } = await supabase
            .rpc('pgis_update_geometry', { 
              table_name: tableName,
              row_id: data.id, 
              geom_json: geometryString 
            });

          if (updateError) {
            console.error(`Error updating geometry in ${tableName}:`, updateError);
            errorCount++;
          } else {
            successCount++;
            // Log progress every 10 features
            if (successCount % 10 === 0) {
              console.log(`Processed ${successCount} features successfully for ${tableName}`);
            }
          }
        } catch (featureError) {
          console.error(`Error processing feature for ${tableName}:`, featureError);
          errorCount++;
        }
      }

      console.log(`Completed table ${tableName}: ${successCount} successful, ${errorCount} errors`);
    }
    
    console.log('All GeoJSON files processed');
    
  } catch (error) {
    console.error('Error reading directory or files:', error);
  }
}

// Create a table for a specific GeoJSON file
async function createTableForGeojson(tableName, geojsonData) {
  try {
    // Get a sample feature to analyze properties
    const sampleFeature = geojsonData.features[0];
    
    // Create the table with dynamic columns based on the first feature's properties
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id SERIAL PRIMARY KEY,
      properties JSONB NOT NULL,
      geometry GEOMETRY
    );
    
    -- Create spatial index on the geometry column
    CREATE INDEX IF NOT EXISTS idx_${tableName}_geometry ON ${tableName} USING GIST (geometry);
    `;
    
    const { error } = await supabase.rpc('exec_sql', { sql: createTableQuery });
    
    if (error) {
      console.error(`Error creating table ${tableName}:`, error);
      throw new Error(`Failed to create table ${tableName}`);
    }
    
    console.log(`Table ${tableName} created successfully`);
    return true;
  } catch (error) {
    console.error(`Error in createTableForGeojson for ${tableName}:`, error);
    return false;
  }
}

// First, create the necessary function in your database
async function setupDatabase() {
  console.log('Setting up database functions...');
  
  // Updated function to work with dynamic table names
  const createFunctionQuery = `
  CREATE OR REPLACE FUNCTION pgis_update_geometry(table_name TEXT, row_id INT, geom_json TEXT)
  RETURNS VOID AS $$
  DECLARE
    sql_query TEXT;
  BEGIN
    sql_query := format('UPDATE %I SET geometry = ST_GeomFromGeoJSON(%L) WHERE id = %L', 
                        table_name, geom_json, row_id);
    EXECUTE sql_query;
  END;
  $$ LANGUAGE plpgsql SECURITY DEFINER;
  
  -- Create exec_sql function if it doesn't exist (for creating tables)
  CREATE OR REPLACE FUNCTION exec_sql(sql TEXT) 
  RETURNS VOID AS $$
  BEGIN
    EXECUTE sql;
  END;
  $$ LANGUAGE plpgsql SECURITY DEFINER;
  `;
  
  // Use raw SQL query since we need to create the function
  const { data, error } = await supabase.rpc('exec_sql', { sql: createFunctionQuery });
  
  if (error) {
    console.error('Error creating functions:', error);
    console.log('You may need to manually create the functions using the SQL editor in Supabase');
  } else {
    console.log('Database functions created successfully');
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