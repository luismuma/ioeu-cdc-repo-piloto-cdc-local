from api.errors import UserInputError

class TrailError(UserInputError):
    pass


def parse_trail(trail: str):
    """
    Accepts:
      et
      north/et
      prod/app1/et

    Enforces:
      - exactly one trail
      - trail name = 2 characters
    """
    
    if "/" in trail:
        path, name = trail.rsplit("/", 1)
    else:
        path, name = "", trail

    if len(name) != 2:
        raise TrailError(
            f"Invalid trail name '{name}'. Must be exactly 2 characters."
        )

    if path.startswith("/"):
        raise TrailError(f"Invalid trail '{trail}'. "
                "Trail paths must be relative, for example: et or test/et")

    if path:
        return {"name": name, "path": path}

    return {"name": name}
   


