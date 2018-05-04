package graph

import (
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"
	"testing"
)

func Test_drawLine(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 1000, 1000))
	draw.Draw(img, img.Bounds(), image.White, image.ZP, draw.Src)
	drawLine(img, image.Pt(10, 10), image.Pt(15, 20), color.RGBA{255, 0, 0, 255})

	f, _ := os.Create("1.png")
	png.Encode(f, img)
	f.Close()
	t.Error(1)
}
