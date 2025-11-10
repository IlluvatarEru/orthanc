from analytics.launch.launch_opportunity_finder import get_all_jks_from_db


class TestJKRentalAnalytics:
    """Test class for JK rentals analytics functionality."""

    def test_analyze_jk_for_rentals_success(self):
        jks = get_all_jks_from_db(city="almaty")
        for jk in jks:
            print(jk)
