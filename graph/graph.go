package graph

import (
	"image"
	"image/color"
	"image/draw"
	"math"
)

func drawLine(img draw.Image, p0, p1 image.Point, clr color.RGBA) {
	drawDashedLine(img, p0, p1, clr, []int{})
}

func drawDashedLine(img draw.Image, p0, p1 image.Point, clr color.RGBA, dasharray []int) {
	// integer part of x
	ipart := func(x float64) int { return int(math.Floor(x)) }
	round := func(x float64) int { return ipart(x + 0.5) }
	// fractional part of x
	fpart := func(x float64) float64 { return x - math.Floor(x) }
	rfpart := func(x float64) float64 { return 1 - fpart(x) }

	plot := func(x, y int, a float64) {
		rgba := img.At(x, y).(color.RGBA)
		r, g, b := float64(clr.R)*a, float64(clr.G)*a, float64(clr.B)*a
		r0, g0, b0 := float64(rgba.R)*(1-a), float64(rgba.G)*(1-a), float64(rgba.B)*(1-a)

		c := func(a, b float64) uint8 {
			if a+b > 255 {
				return 255
			}
			return uint8(a + b)
		}

		rgba.R, rgba.G, rgba.B, rgba.A = c(r, r0), c(g, g0), c(b, b0), 255
		img.Set(x, y, rgba)
	}

	x0, y0 := float64(p0.X), float64(p0.Y)
	x1, y1 := float64(p1.X), float64(p1.Y)
	steep := math.Abs(y1-y0) > math.Abs(x1-x0)

	if steep {
		x0, y0 = y0, x0
		x1, y1 = y1, x1
	}
	if x0 > x1 {
		x0, x1 = x1, x0
		y0, y1 = y1, y0
	}

	dx, dy, gradient := x1-x0, y1-y0, 0.0
	if dx == 0.0 {
		gradient = 1.0
	} else {
		gradient = dy / dx
		if gradient == 1.0 {
			// diagonal line
			for x := int(x0); x <= int(x1); x++ {
				y := int(y0) + x - int(x0)
				plot(x, y, 1)
				plot(x+1, y, 1.0/8)
				plot(x-1, y, 1.0/8)
				plot(x, y+1, 1.0/8)
				plot(x, y-1, 1.0/8)
			}
			return
		}
	}

	// handle first endpoint
	xend := round(x0)
	yend := y0 + gradient*(float64(xend)-x0)
	xgap := rfpart(x0 + 0.5)
	xpxl1 := xend // this will be used in the main loop
	ypxl1 := ipart(yend)
	if steep {
		plot(ypxl1, xpxl1, rfpart(yend)*xgap)
		plot(ypxl1+1, xpxl1, fpart(yend)*xgap)
	} else {
		plot(xpxl1, ypxl1, rfpart(yend)*xgap)
		plot(xpxl1, ypxl1+1, fpart(yend)*xgap)
	}
	intery := yend + gradient // first y-intersection for the main loop

	// handle second endpoint
	xend = round(x1)
	yend = y1 + gradient*(float64(xend)-x1)
	xgap = fpart(x1 + 0.5)
	xpxl2 := xend //this will be used in the main loop
	ypxl2 := ipart(yend)
	if steep {
		plot(ypxl2, xpxl2, rfpart(yend)*xgap)
		plot(ypxl2+1, xpxl2, fpart(yend)*xgap)
	} else {
		plot(xpxl2, ypxl2, rfpart(yend)*xgap)
		plot(xpxl2, ypxl2+1, fpart(yend)*xgap)
	}

	// main loop
	first := ipart(intery)
	if steep {
		for x := xpxl1 + 1; x < xpxl2; x++ {
			if (ipart(intery)-first)%2 < 1 {
				plot(ipart(intery), x, rfpart(intery))
				plot(ipart(intery)+1, x, fpart(intery))
			}
			intery = intery + gradient
		}
	} else {
		for x := xpxl1 + 1; x < xpxl2; x++ {
			if (ipart(intery)-first)%2 < 1 {
				plot(x, ipart(intery), rfpart(intery))
				plot(x, ipart(intery)+1, fpart(intery))
			}
			intery = intery + gradient
		}
	}
}
