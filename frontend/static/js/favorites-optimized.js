/**
 * Optimized favorites management to reduce API calls
 */

class FavoritesManager {
    constructor() {
        this.favoriteStatusCache = new Map();
        this.pendingChecks = new Set();
    }

    /**
     * Check favorite status for multiple flats in one API call
     */
    async checkFavoritesBatch(flats) {
        const flatsToCheck = flats.filter(flat => 
            flat.flat_id && flat.flat_type && !this.favoriteStatusCache.has(flat.flat_id)
        );

        if (flatsToCheck.length === 0) {
            return;
        }

        try {
            const response = await fetch('/api/favorites/check-batch', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    flats: flatsToCheck
                })
            });

            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    // Cache the results
                    Object.entries(data.results).forEach(([flatId, isFavorite]) => {
                        this.favoriteStatusCache.set(flatId, isFavorite);
                    });
                }
            }
        } catch (error) {
            console.error('Error checking favorites batch:', error);
        }
    }

    /**
     * Get favorite status from cache or return false
     */
    isFavorite(flatId) {
        return this.favoriteStatusCache.get(flatId) || false;
    }

    /**
     * Update favorite status in cache
     */
    setFavorite(flatId, isFavorite) {
        this.favoriteStatusCache.set(flatId, isFavorite);
    }

    /**
     * Initialize favorites for all buttons on the page
     */
    async initializeFavorites() {
        const favoriteButtons = document.querySelectorAll('.toggle-favorite');
        const flats = [];

        // Collect all flat data
        favoriteButtons.forEach(button => {
            const flatId = button.getAttribute('data-flat-id');
            const flatType = button.getAttribute('data-flat-type');
            
            if (flatId && flatType) {
                flats.push({
                    flat_id: flatId,
                    flat_type: flatType
                });
            }
        });

        // Check all favorites in one batch
        await this.checkFavoritesBatch(flats);

        // Update button states
        favoriteButtons.forEach(button => {
            const flatId = button.getAttribute('data-flat-id');
            const isFavorite = this.isFavorite(flatId);
            
            this.updateButtonState(button, isFavorite);
        });

        // Add click handlers
        this.addClickHandlers(favoriteButtons);
    }

    /**
     * Update button visual state
     */
    updateButtonState(button, isFavorite) {
        if (isFavorite) {
            button.classList.add('btn-warning');
            button.classList.remove('btn-outline-warning');
        } else {
            button.classList.remove('btn-warning');
            button.classList.add('btn-outline-warning');
        }
    }

    /**
     * Add click handlers to favorite buttons
     */
    addClickHandlers(buttons) {
        buttons.forEach(button => {
            button.addEventListener('click', async (e) => {
                e.preventDefault();
                
                const flatId = button.getAttribute('data-flat-id');
                const flatType = button.getAttribute('data-flat-type');
                const isCurrentlyFavorite = this.isFavorite(flatId);

                if (!flatId || !flatType) {
                    console.error('Missing flat ID or type');
                    return;
                }

                try {
                    const url = isCurrentlyFavorite ? '/api/favorites/remove' : '/api/favorites/add';
                    const requestBody = isCurrentlyFavorite ? 
                        { flat_id: flatId, flat_type: flatType } :
                        { 
                            flat_id: flatId, 
                            flat_type: flatType, 
                            flat_data: this.getFlatData(button)
                        };

                    const response = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify(requestBody)
                    });

                    if (response.ok) {
                        const data = await response.json();
                        if (data.success) {
                            // Update cache and button state
                            this.setFavorite(flatId, !isCurrentlyFavorite);
                            this.updateButtonState(button, !isCurrentlyFavorite);
                        } else if (data.already_favorited) {
                            // Already in favorites - ensure button state is correct
                            this.setFavorite(flatId, true);
                            this.updateButtonState(button, true);
                            console.log(data.message || 'Already in favorites');
                        } else {
                            console.error('Failed to update favorite:', data.message || data.error);
                        }
                    }
                } catch (error) {
                    console.error('Error updating favorite:', error);
                }
            });
        });
    }

    /**
     * Extract flat data from button attributes
     */
    getFlatData(button) {
        return {
            flat_id: button.getAttribute('data-flat-id'),
            price: parseInt(button.getAttribute('data-price')) || 0,
            area: parseFloat(button.getAttribute('data-area')) || 0,
            residential_complex: button.getAttribute('data-residential-complex') || '',
            floor: button.getAttribute('data-floor') || null,
            total_floors: button.getAttribute('data-total-floors') || null,
            construction_year: button.getAttribute('data-construction-year') || null,
            parking: button.getAttribute('data-parking') || '',
            description: button.getAttribute('data-description') || '',
            url: button.getAttribute('data-url') || `https://krisha.kz/a/show/${button.getAttribute('data-flat-id')}`
        };
    }
}

// Global instance
window.favoritesManager = new FavoritesManager();

// Auto-initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    window.favoritesManager.initializeFavorites();
});
