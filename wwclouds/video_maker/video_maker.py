import cv2


class VideoMaker:
    def __init__(self, dest_path: str, image_paths: list[str], fps: int):
        self.dest_path = dest_path
        self.image_paths = image_paths
        self.fps = fps

    def create_video(self):
        start_frame = cv2.imread(self.image_paths[0])
        height, width, layers = start_frame.shape
        fourcc = cv2.VideoWriter_fourcc('m', 'p', '4', 'v')
        video = cv2.VideoWriter(self.dest_path, fourcc, self.fps, (width, height))
        for image in self.image_paths:
            frame = cv2.imread(image)
            video.write(frame)
