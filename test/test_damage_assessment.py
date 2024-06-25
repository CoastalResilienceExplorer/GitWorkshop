from src.damage_assessment import main as damage_assessment

import xarray as xr
import rioxarray as rxr

def test_damage_assessment():
    # Load the flooding data from the ./data directory
    flooding_data = rxr.open_rasterio('./test/data/WaterDepth_Future2050_S4_Tr100_t33.tif')
    
    # Call the damage_assessment function with the flooding data
    result = damage_assessment(flooding_data)
    
    # Perform assertions to check if the result is as expected
    assert result is not None, "The result should not be None"
    assert isinstance(result, xr.DataArray), "The result should be an xarray DataArray"
    assert result.shape == flooding_data.shape, "The result should have the same shape as the input flooding data"
    
    # Additional checks can be added here based on the expected properties of the result
