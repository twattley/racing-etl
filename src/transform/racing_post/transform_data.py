def convert_headgear(e: str) -> list:
    headgear_mapping = {
        "b": "blinkers",
        "t": "tongue tie",
        "p": "cheekpieces",
        "c": "cheekpieces",
        "v": "visor",
        "h": "hood",
        "e/s": "eye shield",
        "e": "eye shield",
    }

    headgear = []
    for i, value in headgear_mapping.items():
        if f"{i}1" in e:
            headgear.append(f"{value} (first time)")
            e = e.replace(f"{i}1", "")
        elif i in e:
            headgear.append(value)
            e = e.replace(i, "")

    first_time_headgear = [i for i in headgear if "first time" in i]
    if first_time_headgear:
        first_time_headgear.extend([i for i in headgear if "first time" not in i])
        return first_time_headgear

    if not headgear and e:
        raise ValueError(f"Unknown headgear code: {e}")
    return headgear
