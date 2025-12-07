def generate_memorable_slug(self) -> str:
    """Generate a memorable slug from the calendar name for fallback use."""
    import re
    
    # If we have a valid slug, use it
    if self.slug and self.slug.strip():
        return self.slug.strip()
    
    # Generate a slug from the name
    name = self.name or "calendar"
    
    # Convert to lowercase and replace spaces/special chars with hyphens
    slug = re.sub(r'[^a-zA-Z0-9\s-]', '', name.lower())
    slug = re.sub(r'[-\s]+', '-', slug).strip('-')
    
    # If still empty or too short, use a default
    if not slug or len(slug) < 3:
        # Use a combination of name and api_id for uniqueness
        name_part = re.sub(r'[^a-zA-Z0-9]', '', name.lower())[:8]
        api_id_part = self.api_id[-4:] if self.api_id else "0000"
        slug = f"{name_part}-{api_id_part}"
    
    return slug

def get_display_name(self) -> str:
    """Get a display name for the calendar, falling back to generated slug if needed."""
    if self.name and self.name.strip():
        return self.name.strip()
    
    # Fallback to generated slug
    return self.generate_memorable_slug()
